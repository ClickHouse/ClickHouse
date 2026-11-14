#pragma once

#include <string>

#include <Parsers/Mongo/ParserMongoSelectQuery.h>
#include <Parsers/Mongo/ParserMongoQuery.h>

namespace DB
{

namespace Mongo
{

class IMongoFunction : public IMongoParser
{
protected:
    explicit IMongoFunction(rapidjson::Value data_, std::shared_ptr<QueryMetadata> metadata_, const std::string & edge_name_)
        : IMongoParser(std::move(data_), metadata_, edge_name_)
    {
    }

public:
    virtual std::string getFunctionName() const = 0;

    ~IMongoFunction() override = default;
};

class MongoIdentityFunction : public IMongoFunction
{
public:
    std::string getFunctionName() const override { return edge_name; }

    explicit MongoIdentityFunction(
        rapidjson::Value array_elements_, std::shared_ptr<QueryMetadata> metadata_, const std::string & edge_name_)
        : IMongoFunction(std::move(array_elements_), metadata_, edge_name_)
    {
    }

    bool parseImpl(ASTPtr & node) override;
};

/// Base class of the filter operators that combine a list of filters: `$and`, `$or` and `$nor`.
class IMongoLogicalFunction : public IMongoFunction
{
public:
    explicit IMongoLogicalFunction(
        rapidjson::Value array_elements_, std::shared_ptr<QueryMetadata> metadata_, const std::string & edge_name_)
        : IMongoFunction(std::move(array_elements_), metadata_, edge_name_)
    {
    }

    virtual std::string getFunctionAlias() const = 0;

    /// `$nor` is `$or` with the result negated.
    virtual bool isNegated() const { return false; }

    bool parseImpl(ASTPtr & node) override;
};

class MongoOrFunction : public IMongoLogicalFunction
{
public:
    std::string getFunctionName() const override { return "$or"; }

    std::string getFunctionAlias() const override { return "or"; }

    explicit MongoOrFunction(rapidjson::Value array_elements_, std::shared_ptr<QueryMetadata> metadata_, const std::string & edge_name_)
        : IMongoLogicalFunction(std::move(array_elements_), metadata_, edge_name_)
    {
    }
};

class MongoAndFunction : public IMongoLogicalFunction
{
public:
    std::string getFunctionName() const override { return "$and"; }

    std::string getFunctionAlias() const override { return "and"; }

    explicit MongoAndFunction(rapidjson::Value array_elements_, std::shared_ptr<QueryMetadata> metadata_, const std::string & edge_name_)
        : IMongoLogicalFunction(std::move(array_elements_), metadata_, edge_name_)
    {
    }
};

/** `{"$expr": <aggregation expression>}` uses the aggregation expression language as a filter,
  * which is how a filter compares two fields of the same document to each other.
  */
class MongoExprFunction : public IMongoFunction
{
public:
    std::string getFunctionName() const override { return "$expr"; }

    explicit MongoExprFunction(rapidjson::Value array_elements_, std::shared_ptr<QueryMetadata> metadata_, const std::string & edge_name_)
        : IMongoFunction(std::move(array_elements_), metadata_, edge_name_)
    {
    }

    bool parseImpl(ASTPtr & node) override;
};

class MongoNorFunction : public IMongoLogicalFunction
{
public:
    std::string getFunctionName() const override { return "$nor"; }

    std::string getFunctionAlias() const override { return "or"; }

    bool isNegated() const override { return true; }

    explicit MongoNorFunction(rapidjson::Value array_elements_, std::shared_ptr<QueryMetadata> metadata_, const std::string & edge_name_)
        : IMongoLogicalFunction(std::move(array_elements_), metadata_, edge_name_)
    {
    }
};

}

}
