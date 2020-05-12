#ifndef CLICKHOUSE_FUNCTIONUDF_H
#define CLICKHOUSE_FUNCTIONUDF_H

#include <Functions/IFunction.h>

#include "UDFConnector.h"

namespace DB
{

class FunctionUDF : public IFunction
{
public:
    explicit FunctionUDF(std::string name_, UDFConnector &connector_) : name(name_), connector(connector_) {}

    std::string getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /* arguments */) const override
    {
        connector.getReturnTypeCall(name); /// @TODO Igr
        // DataTypeFactory::instance();
        throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {}; } /// @TODO Igr

    void executeImpl(Block & /* block */, const ColumnNumbers & /* arguments */, size_t /* result */, size_t /* input_rows_count */) override
    {
        connector.execCall(name); /// @TODO Igr
        throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

private:
    std::string name;
    UDFConnector &connector;
};

}

#endif //CLICKHOUSE_FUNCTIONUDF_H
