#pragma once

#include <Parsers/MySQLCompatibility/AST_fwd.h>
#include <Parsers/MySQLCompatibility/ConversionTree.h>

namespace MySQLCompatibility
{

class IRecognizer
{
public:
	virtual ConvPtr Recognize(MySQLPtr node) const = 0;
	virtual ~IRecognizer() {}
};

using IRecognizerPtr = std::shared_ptr<IRecognizer>;

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

class GenericRecognizer : public IRecognizer
{
public:
	virtual ConvPtr Recognize(MySQLPtr node) const override;
private:
	std::vector<IRecognizerPtr> rules = {
		std::make_shared<SetQueryRecognizer>(),
		std::make_shared<SimpleSelectQueryRecognizer>()
	};
};

}
