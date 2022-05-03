#pragma once

#include <Parsers/MySQLCompatibility/types.h>

namespace MySQLCompatibility
{

class IConversionTree
{
public:
	IConversionTree(MySQLPtr source) : _source(source) {}
	virtual bool setup() = 0;
	virtual void convert(CHPtr & ch_tree) const = 0;
	virtual ~IConversionTree() {}
protected:
	MySQLPtr _source;
};
}
