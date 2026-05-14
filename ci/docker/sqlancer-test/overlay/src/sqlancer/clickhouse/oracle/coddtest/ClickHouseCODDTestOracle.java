package sqlancer.clickhouse.oracle.coddtest;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.clickhouse.ClickHouseErrors;
import sqlancer.clickhouse.ClickHouseProvider.ClickHouseGlobalState;
import sqlancer.clickhouse.ClickHouseSchema.ClickHouseTable;
import sqlancer.clickhouse.ClickHouseVisitor;
import sqlancer.clickhouse.ast.ClickHouseColumnReference;
import sqlancer.clickhouse.ast.ClickHouseExpression;
import sqlancer.clickhouse.ast.ClickHouseSelect;
import sqlancer.clickhouse.ast.ClickHouseTableReference;
import sqlancer.clickhouse.gen.ClickHouseExpressionGenerator;
import sqlancer.common.oracle.CODDTestBase;
import sqlancer.common.oracle.TestOracle;

/**
 * Cross-Optimization Decision Differential Testing for ClickHouse.
 *
 * Runs the same query twice with a random subset of optimizer flags toggled on
 * vs off and asserts the two result sets are identical. Mismatches surface
 * optimizer rewrites that drop or duplicate rows. Flags are injected as a
 * per-query {@code SETTINGS} clause so neighbouring oracle runs sharing the
 * same connection don't see the toggled values.
 *
 * The flag list is deliberately conservative: rewrites with high blast radius
 * (analyzer enable/disable, JOIN algorithm) are excluded because they tend to
 * surface stylistic differences (e.g. NULL ordering inside subqueries) rather
 * than correctness bugs, and would only generate false positives at this layer.
 */
public class ClickHouseCODDTestOracle extends CODDTestBase<ClickHouseGlobalState>
        implements TestOracle<ClickHouseGlobalState> {

    /**
     * Optimizer settings that should be result-preserving regardless of value.
     * If a query returns different rows with the flag on vs off, that's a bug.
     */
    private static final List<String> OPTIMIZER_FLAGS = Arrays.asList(
            "enable_optimize_predicate_expression",
            "optimize_move_to_prewhere",
            "optimize_read_in_order",
            "optimize_aggregation_in_order",
            "optimize_arithmetic_operations_in_aggregate_functions",
            "optimize_functions_to_subcolumns",
            "optimize_substitute_columns",
            "optimize_or_like_chain",
            "optimize_if_chain_to_multiif",
            "optimize_redundant_functions_in_order_by",
            "optimize_trivial_count_query",
            "optimize_using_constraints");

    public ClickHouseCODDTestOracle(ClickHouseGlobalState state) {
        super(state);
        ClickHouseErrors.addExpectedExpressionErrors(this.errors);
    }

    @Override
    public void check() throws Exception {
        List<ClickHouseTable> tables = state.getSchema().getRandomTableNonEmptyTables().getTables();
        if (tables.isEmpty()) {
            throw new IgnoreMeException();
        }
        ClickHouseTableReference table = new ClickHouseTableReference(Randomly.fromList(tables), null);
        List<ClickHouseColumnReference> cols = table.getColumnReferences();
        if (cols.isEmpty()) {
            throw new IgnoreMeException();
        }

        ClickHouseExpressionGenerator gen = new ClickHouseExpressionGenerator(state);
        gen.addColumns(cols);

        ClickHouseSelect select = new ClickHouseSelect();
        select.setFromClause(table);
        select.setFetchColumns(cols.stream().map(c -> (ClickHouseExpression) c).collect(Collectors.toList()));
        if (Randomly.getBoolean()) {
            select.setWhereClause(gen.generateExpressionWithColumns(cols, 4));
        }

        String baseQuery = ClickHouseVisitor.asString(select);

        List<String> flags = Randomly.nonEmptySubset(OPTIMIZER_FLAGS);
        String settingsOn = " SETTINGS " + flags.stream().map(f -> f + " = 1").collect(Collectors.joining(", "));
        String settingsOff = " SETTINGS " + flags.stream().map(f -> f + " = 0").collect(Collectors.joining(", "));

        originalQueryString = baseQuery + settingsOn;
        auxiliaryQueryString = baseQuery + settingsOff;

        List<String> resultOn = run(originalQueryString);
        List<String> resultOff = run(auxiliaryQueryString);

        if (!resultOn.equals(resultOff)) {
            throw new AssertionError(String.format(
                    "CODDTest result mismatch (toggled flags: %s):%n  ON : %s%n  OFF: %s%n  ON  result: %s%n  OFF result: %s",
                    flags, originalQueryString, auxiliaryQueryString, resultOn, resultOff));
        }
    }

    private List<String> run(String query) throws SQLException {
        List<String> rows = new ArrayList<>();
        try (Statement s = state.getConnection().createStatement();
                ResultSet rs = s.executeQuery(query)) {
            ResultSetMetaData md = rs.getMetaData();
            int colCount = md.getColumnCount();
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int i = 1; i <= colCount; i++) {
                    if (i > 1) {
                        row.append('|');
                    }
                    String v = rs.getString(i);
                    row.append(rs.wasNull() ? "NULL" : v);
                }
                rows.add(row.toString());
            }
        } catch (SQLException ex) {
            if (ex.getMessage() != null && errors.errorIsExpected(ex.getMessage())) {
                throw new IgnoreMeException();
            }
            throw ex;
        }
        Collections.sort(rows);
        return rows;
    }
}
