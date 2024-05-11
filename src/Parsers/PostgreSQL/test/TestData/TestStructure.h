namespace DB::PostgreSQL::Testing
{
    struct Test{
        std::string Query, PGAST, ClickHouseAST;
    };
}
