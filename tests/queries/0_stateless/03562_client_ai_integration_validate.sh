#!/usr/bin/env bash
# Tags: no-parallel

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Create test tables with predetermined data
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ai_test_employees" >/dev/null 2>&1
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ai_test_departments" >/dev/null 2>&1
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ai_test_sales" >/dev/null 2>&1

$CLICKHOUSE_CLIENT --query "
CREATE TABLE ai_test_employees (
    id UInt32,
    name String,
    department_id UInt32,
    salary UInt32,
    hire_date Date
) ENGINE = MergeTree()
ORDER BY id"

$CLICKHOUSE_CLIENT --query "
CREATE TABLE ai_test_departments (
    id UInt32,
    name String,
    budget UInt32
) ENGINE = MergeTree()
ORDER BY id"

$CLICKHOUSE_CLIENT --query "
CREATE TABLE ai_test_sales (
    id UInt32,
    employee_id UInt32,
    amount Decimal(10, 2),
    sale_date Date,
    product String
) ENGINE = MergeTree()
ORDER BY (employee_id, sale_date)"

# Insert predetermined data
$CLICKHOUSE_CLIENT --query "
INSERT INTO ai_test_departments VALUES
    (1, 'Engineering', 500000),
    (2, 'Sales', 300000),
    (3, 'Marketing', 200000)"

$CLICKHOUSE_CLIENT --query "
INSERT INTO ai_test_employees VALUES
    (1, 'John Doe', 1, 80000, '2020-01-15'),
    (2, 'Jane Smith', 2, 75000, '2020-03-20'),
    (3, 'Bob Johnson', 1, 90000, '2019-11-10'),
    (4, 'Alice Brown', 3, 65000, '2021-02-01'),
    (5, 'Charlie Wilson', 2, 70000, '2020-06-15')"

$CLICKHOUSE_CLIENT --query "
INSERT INTO ai_test_sales VALUES
    (1, 2, 5000.00, '2024-01-10', 'Widget A'),
    (2, 2, 7500.00, '2024-01-15', 'Widget B'),
    (3, 5, 3000.00, '2024-01-12', 'Widget A'),
    (4, 2, 12000.00, '2024-01-20', 'Widget C'),
    (5, 5, 8000.00, '2024-01-18', 'Widget B')"

# Test 1: Count employees - should generate COUNT query
echo "Test 1: Count employees"
result=$($CLICKHOUSE_CLIENT -q "?? count all employees in ai_test_employees table" 2>&1)
if echo "$result" | grep -qi "count" && echo "$result" | grep -qi "ai_test_employees"; then
    echo "PASS"
else
    echo "FAIL: Expected COUNT and ai_test_employees in generated query"
fi

# Test 2: Maximum salary - should generate MAX query
echo "Test 2: Maximum salary"
result=$($CLICKHOUSE_CLIENT -q "?? find the highest salary from ai_test_employees" 2>&1)
if echo "$result" | grep -qi "max" && echo "$result" | grep -qi "salary"; then
    echo "PASS"
else
    echo "FAIL: Expected MAX and salary in generated query"
fi

# Test 3: Department count - should generate COUNT query on departments
echo "Test 3: Department count"
result=$($CLICKHOUSE_CLIENT -q "?? how many departments exist in ai_test_departments" 2>&1)
if echo "$result" | grep -qi "count" && echo "$result" | grep -qi "ai_test_departments"; then
    echo "PASS"
else
    echo "FAIL: Expected COUNT and ai_test_departments in generated query"
fi

# Test 4: Total sales - should generate SUM query
echo "Test 4: Total sales amount"
result=$($CLICKHOUSE_CLIENT -q "?? calculate total sales amount from ai_test_sales" 2>&1)
if echo "$result" | grep -qi "sum" && echo "$result" | grep -qi "amount"; then
    echo "PASS"
else
    echo "FAIL: Expected SUM and amount in generated query"
fi

# Test 5: Join query - should generate JOIN between tables
echo "Test 5: Employee count by department"
result=$($CLICKHOUSE_CLIENT -q "?? count employees in Engineering department using ai_test_employees and ai_test_departments tables" 2>&1)
if echo "$result" | grep -qi "join" && echo "$result" | grep -qi "engineering"; then
    echo "PASS"
else
    echo "FAIL: Expected JOIN and Engineering in generated query"
fi

# Test 6: Filter by specific value - should use WHERE clause
echo "Test 6: Sales count for specific employee"
result=$($CLICKHOUSE_CLIENT -q "?? count how many sales Jane Smith employee_id 2 made in ai_test_sales" 2>&1)
if echo "$result" | grep -qi "where" && echo "$result" | grep -qi "employee_id"; then
    echo "PASS"
else
    echo "FAIL: Expected WHERE and employee_id in generated query"
fi

# Test 7: Date filtering - should filter by year
echo "Test 7: Date filtering"
result=$($CLICKHOUSE_CLIENT -q "?? count employees hired in 2020 from ai_test_employees" 2>&1)
if echo "$result" | grep -qi "2020" && echo "$result" | grep -qi "hire_date"; then
    echo "PASS"
else
    echo "FAIL: Expected 2020 and hire_date in generated query"
fi

# Test 8: String matching - should filter by product name
echo "Test 8: String matching"
result=$($CLICKHOUSE_CLIENT -q "?? count how many sales were for Widget A in ai_test_sales" 2>&1)
if echo "$result" | grep -qi "widget a" && echo "$result" | grep -qi "product"; then
    echo "PASS"
else
    echo "FAIL: Expected 'Widget A' and product in generated query"
fi

# Test 9: Group by query - should use GROUP BY
echo "Test 9: Group by query"
result=$($CLICKHOUSE_CLIENT -q "?? show total sales by employee_id from ai_test_sales" 2>&1)
if echo "$result" | grep -qi "group by" && echo "$result" | grep -qi "employee_id"; then
    echo "PASS"
else
    echo "FAIL: Expected GROUP BY and employee_id in generated query"
fi

# Test 10: Verify AI generates valid SQL
echo "Test 10: SQL generation validation"
result=$($CLICKHOUSE_CLIENT -q "?? show the SQL to count employees" 2>&1)
if echo "$result" | grep -q "SQL query generated successfully"; then
    echo "PASS"
else
    echo "FAIL: Expected 'SQL query generated successfully' in output"
fi

# Cleanup
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ai_test_employees" >/dev/null 2>&1
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ai_test_departments" >/dev/null 2>&1
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS ai_test_sales" >/dev/null 2>&1