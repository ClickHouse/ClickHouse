#pragma once

#include <Parsers/MySQLCompatibility/AST_fwd.h>

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

class SetQueryCT : public IConversionTree
{
public:
	SetQueryCT(MySQLPtr source) : IConversionTree(source) {}
	virtual bool setup() override;
	virtual void convert(CHPtr & ch_tree) const override;
private:
	std::vector<std::pair<String, String> > _key_value_list;
};

class SimpleSelectQueryCT : public IConversionTree
{
public:
	SimpleSelectQueryCT(MySQLPtr source) : IConversionTree(source) {}
	virtual bool setup() override;
	virtual void convert(CHPtr & ch_tree) const override;
private:
	std::vector<String> columns;
	std::vector<String> tables;
};

}
