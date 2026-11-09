"""File.short_path selectors for Boost.MySQL's upstream separate-compilation library."""

BOOST_MYSQL_COMPILER_SOURCES = [
    "../{}/libs/mysql/test/common/src/{}.cpp".format(Label("@clickhouse_boost//:boost_mysql_compiled").repo_name, name)
    for name in ["boost_asio", "boost_mysql"]
]
