package sqlancer.clickhouse.oracle.pqs;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.clickhouse.ClickHouseErrors;
import sqlancer.clickhouse.ClickHouseProvider.ClickHouseGlobalState;
import sqlancer.clickhouse.ClickHouseSchema;
import sqlancer.clickhouse.ClickHouseSchema.ClickHouseColumn;
import sqlancer.clickhouse.ClickHouseSchema.ClickHouseTable;
import sqlancer.clickhouse.ClickHouseSchema.ClickHouseTables;
import sqlancer.clickhouse.ClickHouseVisitor;
import sqlancer.clickhouse.ast.ClickHouseColumnReference;
import sqlancer.clickhouse.ast.ClickHouseConstant;
import sqlancer.clickhouse.ast.ClickHouseExpression;
import sqlancer.clickhouse.ast.ClickHouseUnaryPostfixOperation;
import sqlancer.clickhouse.ast.ClickHouseUnaryPostfixOperation.ClickHouseUnaryPostfixOperator;
import sqlancer.clickhouse.ast.ClickHouseUnaryPrefixOperation;
import sqlancer.clickhouse.ast.ClickHouseUnaryPrefixOperation.ClickHouseUnaryPrefixOperator;
import sqlancer.clickhouse.gen.ClickHouseExpressionGenerator;
import sqlancer.common.oracle.PivotedQuerySynthesisBase;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.Query;
import sqlancer.common.query.SQLQueryAdapter;

/**
 * Pivoted Query Synthesis (PQS) for ClickHouse, following Rigger & Su, OSDI 2020.
 *
 * The classical SQLancer PQS implementation (e.g. SQLite3) requires every AST
 * node to expose a Java-side {@code getExpectedValue()} that mirrors the DBMS'
 * evaluation semantics. ClickHouse's fork does not provide that for most
 * generated expressions, and reproducing all of ClickHouse's coercion / NULL /
 * arithmetic rules in Java would be an open-ended effort.
 *
 * Instead we delegate rectification to the server: for each randomly generated
 * predicate we ask ClickHouse what the predicate evaluates to on the pivot row
 * by embedding the pivot row's values as literals in a one-row subquery and
 * running the predicate against it. Based on the TRUE / FALSE / NULL answer we
 * either keep the predicate, negate it, or wrap it in {@code IS NULL} so that
 * the conjunction is guaranteed to hold for the pivot row.
 *
 * Containment is checked with {@code INTERSECT}, which treats NULLs as equal
 * in ClickHouse and so handles nullable columns without explicit
 * {@code IS NOT DISTINCT FROM} comparisons.
 */
public class ClickHousePivotedQuerySynthesisOracle extends
        PivotedQuerySynthesisBase<ClickHouseGlobalState, ClickHousePivotedQuerySynthesisOracle.PivotRowValue, ClickHouseExpression, SQLConnection> {

    /**
     * Subclass of {@link sqlancer.common.schema.AbstractRowValue} that exposes
     * a public constructor in this package. The fork's
     * {@code ClickHouseSchema.ClickHouseRowValue} constructor is package-private
     * and so unreachable from {@code sqlancer.clickhouse.oracle.pqs}.
     */
    public static final class PivotRowValue
            extends sqlancer.common.schema.AbstractRowValue<ClickHouseTables, ClickHouseColumn, sqlancer.clickhouse.ast.ClickHouseConstant> {
        public PivotRowValue(ClickHouseTables tables,
                Map<ClickHouseColumn, sqlancer.clickhouse.ast.ClickHouseConstant> values) {
            super(tables, values);
        }
    }

    private ClickHouseTable pivotTable;
    private LinkedHashMap<ClickHouseColumn, ClickHouseConstant> pivotValues;
    private final ExpectedErrors expectedErrors;

    public ClickHousePivotedQuerySynthesisOracle(ClickHouseGlobalState globalState) {
        super(globalState);
        this.expectedErrors = ExpectedErrors.newErrors().with(ClickHouseErrors.getExpectedExpressionErrors()).build();
        this.errors.addAll(ClickHouseErrors.getExpectedExpressionErrors());
    }

    @Override
    protected Query<SQLConnection> getRectifiedQuery() throws Exception {
        rectifiedPredicates.clear();
        pivotRowExpression.clear();

        ClickHouseSchema schema = globalState.getSchema();
        List<ClickHouseTable> nonEmpty = schema.getRandomTableNonEmptyTables().getTables();
        if (nonEmpty.isEmpty()) {
            throw new IgnoreMeException();
        }
        pivotTable = Randomly.fromList(nonEmpty);
        List<ClickHouseColumn> columns = pivotTable.getColumns();
        if (columns.isEmpty()) {
            throw new IgnoreMeException();
        }

        pivotValues = fetchPivotRow(pivotTable, columns);

        // Build the row value used by the base class for diagnostics.
        ClickHouseTables singleTable = new ClickHouseTables(List.of(pivotTable));
        Map<ClickHouseColumn, ClickHouseConstant> rowMap = new LinkedHashMap<>(pivotValues);
        pivotRow = new PivotRowValue(singleTable, rowMap);

        List<ClickHouseColumnReference> columnRefs = columns.stream().map(c -> c.asColumnReference(pivotTable.getName()))
                .collect(Collectors.toList());

        ClickHouseExpressionGenerator gen = new ClickHouseExpressionGenerator(globalState);
        gen.addColumns(columnRefs);

        int nrPredicates = 1 + Randomly.smallNumber();
        List<ClickHouseExpression> rectified = new ArrayList<>();
        for (int i = 0; i < nrPredicates; i++) {
            ClickHouseExpression pred = gen.generateExpressionWithColumns(columnRefs, 4);
            ClickHouseExpression r = rectifyAgainstPivot(pred);
            rectified.add(r);
            rectifiedPredicates.add(r);
        }

        StringBuilder sb = new StringBuilder("SELECT ");
        sb.append(columns.stream().map(c -> quote(pivotTable.getName()) + "." + quote(c.getName()))
                .collect(Collectors.joining(", ")));
        sb.append(" FROM ").append(quote(pivotTable.getName()));
        sb.append(" WHERE ");
        sb.append(rectified.stream().map(p -> "(" + ClickHouseVisitor.asString(p) + ")")
                .collect(Collectors.joining(" AND ")));

        return new SQLQueryAdapter(sb.toString(), expectedErrors);
    }

    @Override
    protected Query<SQLConnection> getContainmentCheckQuery(Query<?> pivotRowQuery) throws Exception {
        StringBuilder sb = new StringBuilder("SELECT ");
        sb.append(pivotValues.values().stream().map(ClickHouseConstant::toString).collect(Collectors.joining(", ")));
        sb.append(" INTERSECT SELECT * FROM (").append(pivotRowQuery.getUnterminatedQueryString()).append(")");
        return new SQLQueryAdapter(sb.toString(), expectedErrors);
    }

    @Override
    protected String getExpectedValues(ClickHouseExpression expr) {
        // ClickHouse expressions don't carry per-node expected values; the
        // base class uses this only for the post-failure diagnostic log.
        return ClickHouseVisitor.asString(expr);
    }

    private LinkedHashMap<ClickHouseColumn, ClickHouseConstant> fetchPivotRow(ClickHouseTable table,
            List<ClickHouseColumn> columns) throws SQLException {
        StringBuilder sb = new StringBuilder("SELECT ");
        sb.append(columns.stream().map(c -> quote(c.getName())).collect(Collectors.joining(", ")));
        sb.append(" FROM ").append(quote(table.getName())).append(" ORDER BY rand() LIMIT 1");

        LinkedHashMap<ClickHouseColumn, ClickHouseConstant> values = new LinkedHashMap<>();
        try (Statement s = globalState.getConnection().createStatement();
                ResultSet rs = s.executeQuery(sb.toString())) {
            if (!rs.next()) {
                // Table is empty even though the schema reported it as non-empty;
                // a concurrent test run may have truncated it.
                throw new IgnoreMeException();
            }
            for (int i = 0; i < columns.size(); i++) {
                ClickHouseColumn c = columns.get(i);
                try {
                    values.put(c, ClickHouseSchema.getConstant(rs, i + 1, c.getType().getType()));
                } catch (AssertionError unsupportedType) {
                    // ClickHouseSchema.getConstant() only knows Int32 / Float64 / String;
                    // for other types skip this pivot attempt rather than fail the run.
                    throw new IgnoreMeException();
                }
            }
        }
        return values;
    }

    /**
     * Asks ClickHouse what the predicate evaluates to on the pivot row, and
     * returns an equivalent expression that is guaranteed to be TRUE on that
     * row: {@code pred} itself if it was TRUE, {@code NOT pred} if it was
     * FALSE, or {@code pred IS NULL} if it was NULL.
     */
    private ClickHouseExpression rectifyAgainstPivot(ClickHouseExpression pred) throws SQLException {
        String predSql = ClickHouseVisitor.asString(pred);

        StringBuilder probe = new StringBuilder("SELECT (").append(predSql).append(") FROM (SELECT ");
        boolean first = true;
        for (Map.Entry<ClickHouseColumn, ClickHouseConstant> e : pivotValues.entrySet()) {
            if (!first) {
                probe.append(", ");
            }
            first = false;
            probe.append(e.getValue().toString()).append(" AS ").append(quote(e.getKey().getName()));
        }
        probe.append(") AS ").append(quote(pivotTable.getName()));

        try (Statement s = globalState.getConnection().createStatement();
                ResultSet rs = s.executeQuery(probe.toString())) {
            if (!rs.next()) {
                throw new IgnoreMeException();
            }
            String raw = rs.getString(1);
            if (rs.wasNull() || raw == null) {
                return new ClickHouseUnaryPostfixOperation(pred, ClickHouseUnaryPostfixOperator.IS_NULL, false);
            }
            if (isTrueValue(raw)) {
                return pred;
            }
            return new ClickHouseUnaryPrefixOperation(pred, ClickHouseUnaryPrefixOperator.NOT);
        } catch (SQLException ex) {
            // Type errors, overflows, regex-compile errors etc. in the
            // randomly-generated predicate are not bugs in ClickHouse — drop
            // this attempt.
            String msg = ex.getMessage();
            if (msg != null && expectedErrors.errorIsExpected(msg)) {
                throw new IgnoreMeException();
            }
            throw ex;
        }
    }

    private static boolean isTrueValue(String raw) {
        try {
            return Double.parseDouble(raw) != 0.0;
        } catch (NumberFormatException nf) {
            return !raw.isEmpty();
        }
    }

    private static String quote(String identifier) {
        return "`" + identifier.replace("`", "``") + "`";
    }
}
