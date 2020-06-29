import os

def argparser(parser):
    """Default argument parser for regressions.
    """
    parser.add_argument("--local", 
        action="store_true",
        help="run regression in local mode", default=False)
    
    parser.add_argument("--clickhouse-binary-path", 
        type=str, dest="clickhouse_binary_path",
        help="path to ClickHouse binary, default: /usr/bin/clickhouse", metavar="path",
        default=os.getenv("CLICKHOUSE_TESTS_SERVER_BIN_PATH", "/usr/bin/clickhouse"))
