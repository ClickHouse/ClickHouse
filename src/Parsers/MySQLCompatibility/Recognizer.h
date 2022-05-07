#pragma once

#include <Parsers/MySQLCompatibility/IConversionTree.h>
#include <Parsers/MySQLCompatibility/types.h>

namespace MySQLCompatibility
{

class IRecognizer
{
public:
    virtual ConvPtr Recognize(MySQLPtr node) const = 0;
    virtual ~IRecognizer() { }
};

class SetQueryRecognizer : public IRecognizer
{
public:
    virtual ConvPtr Recognize(MySQLPtr node) const override;
};

class SelectQueryRecognizer : public IRecognizer
{
public:
    virtual ConvPtr Recognize(MySQLPtr node) const override;
};

class UseCommandRecognizer : public IRecognizer
{
public:
    virtual ConvPtr Recognize(MySQLPtr node) const override;
};

class ShowTablesQueryRecognizer : public IRecognizer
{
public:
    virtual ConvPtr Recognize(MySQLPtr node) const override;
};

using IRecognizerPtr = std::shared_ptr<IRecognizer>;

class GenericRecognizer : public IRecognizer
{
public:
    virtual ConvPtr Recognize(MySQLPtr node) const override;

private:
    std::vector<IRecognizerPtr> rules
        = {std::make_shared<SetQueryRecognizer>(),
           std::make_shared<SelectQueryRecognizer>(),
           std::make_shared<UseCommandRecognizer>(),
           std::make_shared<ShowTablesQueryRecognizer>()};
};

}
