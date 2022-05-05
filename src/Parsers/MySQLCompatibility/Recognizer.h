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

class SimpleSelectQueryRecognizer : public IRecognizer
{
public:
    virtual ConvPtr Recognize(MySQLPtr node) const override;
};

class UseCommandRecognizer : public IRecognizer
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
    std::vector<IRecognizerPtr> rules = {
        std::make_shared<SetQueryRecognizer>(), std::make_shared<SimpleSelectQueryRecognizer>(), std::make_shared<UseCommandRecognizer>()};
};

}
