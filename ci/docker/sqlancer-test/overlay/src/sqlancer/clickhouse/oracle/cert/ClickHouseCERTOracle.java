package sqlancer.clickhouse.oracle.cert;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.clickhouse.ClickHouseErrors;
import sqlancer.clickhouse.ClickHouseProvider.ClickHouseGlobalState;
import sqlancer.clickhouse.ClickHouseSchema.ClickHouseTable;
import sqlancer.clickhouse.ClickHouseVisitor;
import sqlancer.clickhouse.ast.ClickHouseBinaryLogicalOperation;
import sqlancer.clickhouse.ast.ClickHouseBinaryLogicalOperation.ClickHouseBinaryLogicalOperator;
import sqlancer.clickhouse.ast.ClickHouseColumnReference;
import sqlancer.clickhouse.ast.ClickHouseExpression;
import sqlancer.clickhouse.ast.ClickHouseSelect;
import sqlancer.clickhouse.ast.ClickHouseSelect.SelectType;
import sqlancer.clickhouse.ast.ClickHouseTableReference;
import sqlancer.clickhouse.gen.ClickHouseExpressionGenerator;
import sqlancer.common.DBMSCommon;
import sqlancer.common.oracle.CERTOracleBase;
import sqlancer.common.oracle.TestOracle;

/**
 * Cardinality Estimation Restriction Testing for ClickHouse.
 *
 * Pattern follows {@code CockroachDBCERTOracle}: build a random {@code SELECT},
 * mutate it through one of the {@code Mutator} hooks with a known monotonicity
 * direction, then assert that the actual row count moves the way the mutation
 * predicts. A structural-similarity gate on {@code EXPLAIN PLAN} skips cases
 * where the query plan diverges enough that the comparison stops being
 * meaningful.
 *
 * ClickHouse doesn't surface single-number cardinality estimates through any
 * {@code EXPLAIN} variant the JDBC client can read, so we use actual row counts
 * — which still catches optimizer-driven row loss (e.g. predicate pushdown bugs
 * dropping rows, AND/OR rewriting producing wrong counts, faulty DISTINCT
 * dedup). The {@code JOIN}, {@code GROUPBY}, {@code HAVING}, {@code LIMIT}
 * mutators are intentionally not wired up: {@code LIMIT} isn't serialized by
 * the fork's visitor, and the others need richer query shapes than the existing
 * generator produces.
 */
public class ClickHouseCERTOracle extends CERTOracleBase<ClickHouseGlobalState>
        implements TestOracle<ClickHouseGlobalState> {

    private ClickHouseExpressionGenerator gen;
    private ClickHouseSelect select;
    private List<ClickHouseColumnReference> columns;

    public ClickHouseCERTOracle(ClickHouseGlobalState state) {
        super(state);
        ClickHouseErrors.addExpectedExpressionErrors(this.errors);
    }

    @Override
    public void check() throws SQLException {
        queryPlan1Sequences = new ArrayList<>();
        queryPlan2Sequences = new ArrayList<>();

        List<ClickHouseTable> tables = state.getSchema().getRandomTableNonEmptyTables().getTables();
        if (tables.isEmpty()) {
            throw new IgnoreMeException();
        }
        ClickHouseTableReference table = new ClickHouseTableReference(Randomly.fromList(tables), null);
        select = new ClickHouseSelect();
        select.setFromClause(table);
        columns = table.getColumnReferences();
        if (columns.isEmpty()) {
            throw new IgnoreMeException();
        }
        gen = new ClickHouseExpressionGenerator(state);
        gen.addColumns(columns);
        select.setFetchColumns(columns.stream().map(c -> (ClickHouseExpression) c).collect(Collectors.toList()));
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpressionWithColumns(columns, 4));
        }

        String q1 = ClickHouseVisitor.asString(select);
        long count1 = countRows(q1);
        queryPlan1Sequences = explainPlan(q1);

        boolean increase = mutate(Mutator.JOIN, Mutator.GROUPBY, Mutator.HAVING, Mutator.LIMIT);

        String q2 = ClickHouseVisitor.asString(select);
        long count2 = countRows(q2);
        queryPlan2Sequences = explainPlan(q2);

        if (queryPlan1Sequences.isEmpty() || queryPlan2Sequences.isEmpty()) {
            return;
        }
        if (DBMSCommon.editDistance(queryPlan1Sequences, queryPlan2Sequences) > 1) {
            return;
        }

        boolean violated = increase ? count1 > count2 : count1 < count2;
        if (violated) {
            throw new AssertionError(String.format(
                    "CERT monotonicity violation: q1=%d rows, q2=%d rows, expected_increase=%s%n  q1: %s%n  q2: %s",
                    count1, count2, increase, q1, q2));
        }
    }

    private long countRows(String query) throws SQLException {
        try (Statement s = state.getConnection().createStatement();
                ResultSet rs = s.executeQuery(query)) {
            long c = 0;
            while (rs.next()) {
                c++;
            }
            return c;
        } catch (SQLException ex) {
            if (ex.getMessage() != null && errors.errorIsExpected(ex.getMessage())) {
                throw new IgnoreMeException();
            }
            throw ex;
        }
    }

    private List<String> explainPlan(String query) {
        List<String> plan = new ArrayList<>();
        try (Statement s = state.getConnection().createStatement();
                ResultSet rs = s.executeQuery("EXPLAIN PLAN " + query)) {
            while (rs.next()) {
                String line = rs.getString(1);
                if (line == null) {
                    continue;
                }
                String op = line.trim().split("[\\s(]", 2)[0];
                if (!op.isEmpty()) {
                    plan.add(op);
                }
            }
        } catch (SQLException ignored) {
            // EXPLAIN may fail for the same reasons the query fails; the
            // outer count step already triggers IgnoreMeException in that
            // case, so just return an empty plan and let the caller skip.
        }
        return plan;
    }

    @Override
    protected boolean mutateWhere() {
        boolean hadWhere = select.getWhereClause() != null;
        if (hadWhere) {
            select.setWhereClause(null);
            return true;
        }
        select.setWhereClause(gen.generateExpressionWithColumns(columns, 4));
        return false;
    }

    @Override
    protected boolean mutateAnd() {
        ClickHouseExpression extra = gen.generateExpressionWithColumns(columns, 3);
        ClickHouseExpression w = select.getWhereClause();
        if (w == null) {
            select.setWhereClause(extra);
        } else {
            select.setWhereClause(new ClickHouseBinaryLogicalOperation(w, extra, ClickHouseBinaryLogicalOperator.AND));
        }
        return false;
    }

    @Override
    protected boolean mutateOr() {
        ClickHouseExpression w = select.getWhereClause();
        ClickHouseExpression extra = gen.generateExpressionWithColumns(columns, 3);
        if (w == null) {
            select.setWhereClause(extra);
            return false;
        }
        select.setWhereClause(new ClickHouseBinaryLogicalOperation(w, extra, ClickHouseBinaryLogicalOperator.OR));
        return true;
    }

    @Override
    protected boolean mutateDistinct() {
        boolean wasDistinct = select.getFromOptions() == SelectType.DISTINCT;
        select.setSelectType(wasDistinct ? SelectType.ALL : SelectType.DISTINCT);
        return wasDistinct;
    }
}
