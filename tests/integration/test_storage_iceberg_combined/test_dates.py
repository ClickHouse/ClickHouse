import datetime


def test_date_reads(engine, node):
    table = engine.unique_table("test_date_reads")
    engine.create_table(table, [("number", "int"), ("date_col", "date")])
    engine.insert(table, [
        (1, datetime.date(2299, 12, 31)),
        (2, datetime.date(1900, 1, 13)),
    ])
    engine.sync(table)

    target = engine.clickhouse_read_target(node, table)

    rows_in_ch = int(node.query(f"SELECT count() FROM {target}").strip())
    assert rows_in_ch == 2, f"Expected 2 rows, but got {rows_in_ch}"

    ret_date_1 = node.query(f"SELECT date_col FROM {target} WHERE number = 1").strip()
    assert ret_date_1 == "2299-12-31", f"got {ret_date_1}"

    ret_date_2 = node.query(f"SELECT date_col FROM {target} WHERE number = 2").strip()
    assert ret_date_2 == "1900-01-13", f"got {ret_date_2}"
