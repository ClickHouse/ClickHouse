## Embedded ClickHouse as a Python module
_clickhousepy_ is a python module written using the pybind11 library and provides Clickhouse functionality.

## Build
The assembly is carried out with the flags 
`-DENABLE_JEMALLOC=0 -DUSE_STATIC_LIBRARIES=0 -DSPLIT_SHARED_LIBRARIES=1 -DDISABLE_HERMETIC_BUILD=1`
resulting file named `clickhousepy.cpython-310-x86_64-linux-gnu.so` will be located in the directory
`build/python_module`

## Querying

The module contains 3 functions - create_table, insert and query.

* `create_table` takes a query string as an argument and creates a table based on it. In this case, the `ENGINE` argument is initialized with the value `Memory`. The request to create a table has the usual SQL format, except that you do not need to write the keywords `CREATE TABLE` and initialize the `ENGINE` yourself.

* `insert` executes queries of the form `INSERT INTO`. As an argument, the table name and the Python dictionary are passed, arranged as follows: `{"table_name" : [value1, value2, ...], }`, that is, the keys are column names, and the values are numpy.array or Python lists. The user himself is responsible for the correctness of the types. The insertion can be carried out in blocks of the same size

* `query` takes a query string as an argument and processes it. The query string is passed in the classic SQL format. `SELECT` queries are supported. As a result, the function returns a Python dictionary consisting of column names and numpy arrays (if these are numeric values) or a list of strings

## Examples

    import clickhousepy

    dogs = {
            "breed" : ["sheepdog", "greyhound"], 
            "dog_weight" : [35.5, 25.0], 
            "number_of_dogs" : [4, 12]
            }

    new_dogs = {
            "breed" : ["bulldog"], 
            "dog_weight" : [25.5], 
            "number_of_dogs" : [5]
            }

    clickhousepy.create_table("`dogs` (`breed` String, `greyhound` Float64, number_of_dogs Int32)")
    clickhousepy.insert("dogs", dogs)

    res = clickhousepy.query("SELECT * FROM dogs")
    print(res)

Output: 

`{'breed': ['sheepdog', 'greyhound'], 'greyhound': array([35.5, 25. ]), 'number_of_dogs': array([ 4, 12], dtype=int32)}`


    clickhousepy.insert("dogs", new_dogs);
    res = clickhousepy.query("SELECT * FROM dogs")
    print(res)

Output:

`{'breed': ['sheepdog', 'greyhound', 'bulldog'], 'greyhound': array([35.5, 25. , 25.5]), 'number_of_dogs': array([ 4, 12,  5], dtype=int32)}`

    res = clickhousepy.query("SELECT breed FROM dogs")
    print(res)


Output:

`{'breed': ['sheepdog', 'greyhound', 'bulldog']}`

## Comparsion to DuckDB
A small comparison of clickhousepy and duckdb Python API:

The main difference from the Duckadam Python API from clickhousepy is the need to create a "connection". Also, the clickhousepy functionality is divided into several functions that perform more specific tasks, DuckDB uses one execute function for this.


|action                  | CliclHouse                   | DuckDB               |
|------------------------|------------------------------|----------------------|
|     create connection  | None                         | duckdb.connect       |
|create table            |clickhousepy.create_table     | con.execute          |
|SELECT quer             | clickhousepy.query           | con.execute          |
