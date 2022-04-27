#pragma once

#include <Parsers/MySQLCompatibility/AST_fwd.h>

// TODO: split into separate files

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

class UseCommandCT : public IConversionTree
{
public:
	UseCommandCT(MySQLPtr source) : IConversionTree(source) {}
	virtual bool setup() override;
	virtual void convert(CHPtr & ch_tree) const override;
private:
	std::string database;
};

class SimpleSelectQueryCT : public IConversionTree
{
public:
	enum class ORDER_BY_DIR : uint8_t
	{
		ASC,
		DESC
	};

	SimpleSelectQueryCT(MySQLPtr source) : IConversionTree(source) {}
	virtual bool setup() override;
	virtual void convert(CHPtr & ch_tree) const override;
private:
	bool has_order_by = false;
	std::vector<std::pair<String, ORDER_BY_DIR> > order_by_args;
	std::vector<String> columns;
	std::vector<String> tables;
};

}
