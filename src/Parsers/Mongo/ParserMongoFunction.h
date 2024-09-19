#pragma once

#include <string>

#include <Parsers/Mongo/ParserMongoSelectQuery.h>

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

class MongoLiteralFunction : public IMongoFunction
{
public:
    std::string getFunctionName() const override { return edge_name; }

    explicit MongoLiteralFunction(
        rapidjson::Value array_elements_, std::shared_ptr<QueryMetadata> metadata_, const std::string & edge_name_)
        : IMongoFunction(std::move(array_elements_), metadata_, edge_name_)
    {
    }

    bool parseImpl(ASTPtr & node) override;
};


class MongoOrFunction : public IMongoFunction
{
public:
    std::string getFunctionName() const override { return "$or"; }

    explicit MongoOrFunction(rapidjson::Value array_elements_, std::shared_ptr<QueryMetadata> metadata_, const std::string & edge_name_)
        : IMongoFunction(std::move(array_elements_), metadata_, edge_name_)
    {
    }

    bool parseImpl(ASTPtr & node) override;
};

/// Base class for arithmetic functions like add, multiplication and others.
class IMongoArithmeticFunction : public IMongoFunction
{
public:
    explicit IMongoArithmeticFunction(
        rapidjson::Value array_elements_, std::shared_ptr<QueryMetadata> metadata_, const std::string & edge_name_)
        : IMongoFunction(std::move(array_elements_), metadata_, edge_name_)
    {
    }

    virtual std::string getFunctionAlias() const = 0;

    bool parseImpl(ASTPtr & node) override;
};

class MongoSumFunction : public IMongoArithmeticFunction
{
public:
    std::string getFunctionName() const override { return "$add"; }

    std::string getFunctionAlias() const override { return "plus"; }

    explicit MongoSumFunction(rapidjson::Value array_elements_, std::shared_ptr<QueryMetadata> metadata_, const std::string & edge_name_)
        : IMongoArithmeticFunction(std::move(array_elements_), metadata_, edge_name_)
    {
    }
};

class MongoMultiplyFunction : public IMongoArithmeticFunction
{
public:
    std::string getFunctionName() const override { return "$mul"; }

    std::string getFunctionAlias() const override { return "multiply"; }

    explicit MongoMultiplyFunction(
        rapidjson::Value array_elements_, std::shared_ptr<QueryMetadata> metadata_, const std::string & edge_name_)
        : IMongoArithmeticFunction(std::move(array_elements_), metadata_, edge_name_)
    {
    }
};

class MongoDivideFunction : public IMongoArithmeticFunction
{
public:
    std::string getFunctionName() const override { return "$div"; }

    std::string getFunctionAlias() const override { return "divide"; }

    explicit MongoDivideFunction(rapidjson::Value array_elements_, std::shared_ptr<QueryMetadata> metadata_, const std::string & edge_name_)
        : IMongoArithmeticFunction(std::move(array_elements_), metadata_, edge_name_)
    {
    }
};

class MongoMinusFunction : public IMongoArithmeticFunction
{
public:
    std::string getFunctionName() const override { return "$sub"; }

    std::string getFunctionAlias() const override { return "minus"; }

    explicit MongoMinusFunction(rapidjson::Value array_elements_, std::shared_ptr<QueryMetadata> metadata_, const std::string & edge_name_)
        : IMongoArithmeticFunction(std::move(array_elements_), metadata_, edge_name_)
    {
    }
};


class MongoArithmeticFunctionElement : public IMongoFunction
{
public:
    std::string getFunctionName() const override { return "$arithmetic_function_element"; }

    explicit MongoArithmeticFunctionElement(
        rapidjson::Value array_elements_, std::shared_ptr<QueryMetadata> metadata_, const std::string & edge_name_)
        : IMongoFunction(std::move(array_elements_), metadata_, edge_name_)
    {
    }

    bool parseImpl(ASTPtr & node) override;
};


}

}
