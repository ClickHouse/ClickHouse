#pragma once

#include <Parsers/Mongo/ParserMongoFunction.h>

namespace DB
{

namespace Mongo
{

class ICompareFunction : public IMongoFunction
{
public:
    explicit ICompareFunction(rapidjson::Value array_elements_, std::shared_ptr<QueryMetadata> metadata_, const std::string & edge_name_)
        : IMongoFunction(std::move(array_elements_), metadata_, edge_name_)
    {
    }

    virtual std::string getFunctionAlias() const = 0;

    bool parseImpl(ASTPtr & node) override;
};

class MongoLtFunction : public ICompareFunction
{
public:
    std::string getFunctionName() const override { return "$lt"; }

    std::string getFunctionAlias() const override { return "less"; }

    explicit MongoLtFunction(rapidjson::Value array_elements_, std::shared_ptr<QueryMetadata> metadata_, const std::string & edge_name_)
        : ICompareFunction(std::move(array_elements_), metadata_, edge_name_)
    {
    }
};


class MongoLteFunction : public ICompareFunction
{
public:
    std::string getFunctionName() const override { return "$lte"; }

    std::string getFunctionAlias() const override { return "lessOrEquals"; }


    explicit MongoLteFunction(rapidjson::Value array_elements_, std::shared_ptr<QueryMetadata> metadata_, const std::string & edge_name_)
        : ICompareFunction(std::move(array_elements_), metadata_, edge_name_)
    {
    }
};

class MongoGtFunction : public ICompareFunction
{
public:
    std::string getFunctionName() const override { return "$gt"; }

    std::string getFunctionAlias() const override { return "greater"; }

    explicit MongoGtFunction(rapidjson::Value array_elements_, std::shared_ptr<QueryMetadata> metadata_, const std::string & edge_name_)
        : ICompareFunction(std::move(array_elements_), metadata_, edge_name_)
    {
    }
};

class MongoGteFunction : public ICompareFunction
{
public:
    std::string getFunctionName() const override { return "$gte"; }

    std::string getFunctionAlias() const override { return "greaterOrEquals"; }

    explicit MongoGteFunction(rapidjson::Value array_elements_, std::shared_ptr<QueryMetadata> metadata_, const std::string & edge_name_)
        : ICompareFunction(std::move(array_elements_), metadata_, edge_name_)
    {
    }
};

class MongoNotEqualsFunction : public ICompareFunction
{
public:
    std::string getFunctionName() const override { return "$ne"; }

    std::string getFunctionAlias() const override { return "notEquals"; }

    explicit MongoNotEqualsFunction(
        rapidjson::Value array_elements_, std::shared_ptr<QueryMetadata> metadata_, const std::string & edge_name_)
        : ICompareFunction(std::move(array_elements_), metadata_, edge_name_)
    {
    }
};

class MongoLikeFunction : public ICompareFunction
{
public:
    std::string getFunctionName() const override { return "$regex"; }

    std::string getFunctionAlias() const override { return "like"; }

    explicit MongoLikeFunction(rapidjson::Value array_elements_, std::shared_ptr<QueryMetadata> metadata_, const std::string & edge_name_)
        : ICompareFunction(std::move(array_elements_), metadata_, edge_name_)
    {
    }
};

}

}
