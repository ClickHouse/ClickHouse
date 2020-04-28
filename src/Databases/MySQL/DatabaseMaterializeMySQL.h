//#pragma once
//
//#include <DataTypes/DataTypeString.h>
//#include <DataTypes/DataTypesNumber.h>
//#include <Databases/IDatabase.h>
//#include <Parsers/ASTCreateQuery.h>
//#include <mysqlxx/Pool.h>
//
//namespace DB
//{
//
//class DatabaseMaterializeMySQL : public IDatabase
//{
//public:
//    DatabaseMaterializeMySQL(
//        const Context & context, const String & database_name_, const String & metadata_path_,
//        const ASTStorage * database_engine_define_, const String & mysql_database_name_, mysqlxx::Pool && pool_);
//
//    String getEngineName() const override { return "MySQL"; }
//
//private:
//    const Context & global_context;
//    String metadata_path;
//    ASTPtr database_engine_define;
//    String mysql_database_name;
//
//    mutable mysqlxx::Pool pool;
//};
//
//}
