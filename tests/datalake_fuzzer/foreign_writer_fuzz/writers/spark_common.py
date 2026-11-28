"""Spark helpers shared by the Spark-based writer adapters."""

import contextlib
import datetime
import itertools

from ..predicates import render, spark_literal

_view_counter = itertools.count()


def q(name):
    return "`" + name.replace("`", "``") + "`"


def ddl(columns, not_null=()):
    return ", ".join(
        f"{q(n)} {t.spark_ddl()}{' NOT NULL' if n in not_null else ''}"
        for n, t in columns
    )


def dataframe(spark, columns, rows, num_partitions=1):
    """A DataFrame with exactly the model types. Python values go through `createDataFrame` with an
    explicit DDL schema, which keeps aware/naive datetimes and decimals intact."""
    data = [tuple(t.to_spark_value(r[n]) for n, t in columns) for r in rows]
    df = spark.createDataFrame(data, schema=ddl(columns))
    if num_partitions > 1 and len(rows) > 1:
        df = df.repartition(num_partitions)
    return df


@contextlib.contextmanager
def temp_view(spark, columns, rows):
    """A session temp view over `rows`, dropped on exit so the shared session does not keep the data."""
    name = f"fwf_src_{next(_view_counter)}"
    dataframe(spark, columns, rows).createOrReplaceTempView(name)
    try:
        yield name
    finally:
        spark.catalog.dropTempView(name)


def where(pred_json):
    from ..predicates import pred_from_json

    return render(pred_from_json(pred_json), "spark")


def set_clause(column, t, value):
    return f"{q(column)} = {spark_literal(t, value)}"


def select_list(columns):
    return ", ".join(q(n) for n, _ in columns)


def to_arrow(df, columns):
    """Read a DataFrame into pyarrow keeping only the model columns, in model order."""
    return df.select(*[df[q(n)] for n, _ in columns]).toArrow()


def now_literal(offset_seconds=1):
    ts = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
        seconds=offset_seconds
    )
    return f"TIMESTAMP'{ts.strftime('%Y-%m-%d %H:%M:%S.%f')}+00:00'"
