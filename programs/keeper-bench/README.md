# Keeper Bench

Keeper Bench is a tool for benchmarking Keeper or any ZooKeeper compatible systems.

To run it call following command from the build folder:

```
./utils/keeper-bench --config benchmark_config_file.yaml
```

## Configuration file

Keeper Bench runs need to be configured inside a yaml or XML file.
An example of a configuration file can be found in `./utils/keeper-bench/example.yaml`

### Table of contents
- [Special Types](#special-types)
- [General settings](#general-settings)
- [Connections](#connections)
- [Generator](#generator)
- [Output](#output)

<a name="special-types"></a>
## Special types

### IntegerGetter

Can be defined with constant integer or as a random value from a range.

```yaml
key: integer
key:
    min_value: integer
    max_value: integer
```

Example for a constant value:

```yaml
some_key: 2
```

Example for random value from [10, 20]:

```yaml
some_key:
    min_value: 10
    max_value: 20
```

### StringGetter

Can be defined with constant string or as a random string of some size.

```yaml
key: string
key:
    random_string:
        size: IntegerGetter
```

Example for a constant value:
```yaml
some_key: "string"
```

Example for a random string with a random size from [10, 20]:
```yaml
some_key:
    random_string:
        size:
            min_value: 10
            max_value: 20
```


### PathGetter

If a section contains one or more `path` keys, all `path` keys are collected into a list. \
Additionally, paths can be defined with key `children_of` which will add all children of some path to the list.

```yaml
path: string
path:
    children_of: string
```

Example for defining list of paths (`/path1`, `/path2` and children of `/path3`):

```yaml
main:
    path:
        - "/path1"
        - "/path2"
    path:
        children_of: "/path3"
```

<a name="general-settings"></a>
## General settings

```yaml
# number of parallel queries (default: 1)
concurrency: integer

# amount of queries to be executed, set 0 to disable limit (default: 0)
iterations: integer

# delay between intermediate reports in seconds, set 0 to disable reports (default: 1.0)
report_delay: double

# stop launch of queries after specified time limit, set 0 to disable limit (default: 0)
timelimit: double

# continue testing even if a query fails (default: false)
continue_on_errors: boolean
```

<a name="connections"></a>
## Connections

Connection definitions that will be used throughout tests defined under `connections` key.

Following configurations can be defined under `connections` key or for each specific connection. \
If it's defined under `connections` key, it will be used by default unless a specific connection overrides it.

```yaml
secure: boolean
operation_timeout_ms: integer
session_timeout_ms: integer
connection_timeout_ms: integer
```

Specific configuration can be defined with a string or with a detailed description.

```yaml
host: string
connection:
    host: string

    # number of sessions to create for host
    sessions: integer
    # any connection configuration defined above
```

Example definition of 3 connections in total, 1 to `localhost:9181` and 2 to `localhost:9182` both will use secure connections:

```yaml
connections:
    secure: true

    host: "localhost:9181"
    connection:
        host: "localhost:9182"
        sessions: 2
```

<a name="generator"></a>
## Generator

Main part of the benchmark is the generator itself which creates necessary nodes and defines how the requests will be generated. \
It is defined under `generator` key.

### Setup

Setup defines nodes that are needed for test, defined under `setup` key.

Each node is defined with a `node` key in the following format:

```yaml
node: StringGetter

node:
    name: StringGetter
    data: StringGetter
    repeat: integer
    node: Node
```

If only string is defined, a node with that name will be created. \
Otherwise more detailed definition could be included to set data or the children of the node. \
If `repeat` key is set, the node definition will be used multiple times. For a `repeat` key to be valid, the name of the node needs to be a random string.

Example for a setup:

```yaml
generator:
    setup:
        node: "node1"
            node:
                name:
                    random_string:
                        size: 20
                data: "somedata"
                repeat: 4
        node:
            name:
                random_string:
                    size: 10
            repeat: 2
```

We will create node `/node1` with no data and 4 children of random name of size 20 and data set to `somedata`. \
We will also create 2 nodes with no data and random name of size 10 under `/` node.

### Requests

While benchmark is running, we are generating requests.

Request generator is defined under `requests` key. \
For each request `weight` (default: 1) can be defined which defines preference for a certain request.

#### `create`

```yaml
create:
    # parent path for created nodes
    path: string

    # length of the name for the create node (default: 5)
    name_length: IntegerGetter

    # data for create nodes (default: "")
    data: StringGetter

    # value in range [0.0, 1.0> denoting how often a remove request should be generated compared to create request (default: 0)
    remove_factor: double
```

#### `set`

```yaml
set:
    # paths on which we randomly set data
    path: PathGetter

    # data to set
    data: StringGetter
```

#### `get`

```yaml
get:
    # paths for which we randomly get data
    path: PathGetter
```

#### `list`

```yaml
list:
    # paths for which we randomly do list request
    path: PathGetter
```

#### `multi`

```yaml
multi:
    # any request definition defined above can be added

    # optional size for the multi request
    size: IntegerGetter
```

Multi request definition can contain any other request generator definitions described above. \
If `size` key is defined, we will randomly pick `size` amount of requests from defined request generators. \
All request generators can have a higher pick probability by using `weight` key. \
If `size` is not defined, multi request with same request generators will always be generated. \
Both write and read multi requests are supported.

#### Example

```yaml
generator:
    requests:
        create:
            path: "/test_create"
            name_length:
                min_value: 10
                max_value: 20
        multi:
            weight: 20
            size: 10
            get:
                path:
                    children_of: "/test_get1"
            get:
                weight: 2
                path:
                    children_of: "/test_get2"
```

We defined a request geneator that will generate either a `create` or a `multi` request. \
Each `create` request will create a node under `/test_create` with a randomly generated name with size from range `[10, 20]`. \
`multi` request will be generated 20 times more than `create` request. \
`multi` request will contain 10 requests and approximately twice as much get requests to children of "/test_get2".

<a name="output"></a>
## Output

```yaml
output:
    # if defined, JSON output of results will be stored at the defined path
    file: string
    # or
    file:
        # if defined, JSON output of results will be stored at the defined path
        path: string

        # if set to true, timestamp will be appended to the output file name (default: false)
        with_timestamp: boolean

    # if set to true, output will be printed to stdout also (default: false)
    stdout: boolean
```
