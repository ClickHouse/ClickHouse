#pragma once

#include <Parsers/MySQLCompatibility/IConversionTree.h>

namespace MySQLCompatibility
{

class SelectItemsListCT : public IConversionTree
{
public:
	SelectItemsListCT(MySQLPtr source) : IConversionTree(source) {}
	virtual bool setup() override;
	virtual void convert(CHPtr & ch_tree) const override;
private:
	std::vector<ConvPtr> exprs;
};

class SelectOrderByCT : public IConversionTree
{
public:
	enum class DIRECTION : uint8_t
	{
		ASC,
		DESC
	};
	SelectOrderByCT(MySQLPtr source) : IConversionTree(source) {}
	virtual bool setup() override;
	virtual void convert(CHPtr & ch_tree) const override;
private:
	std::vector<std::pair<String, DIRECTION> > args;
};

class SelectLimitOffsetCT : public IConversionTree
{
public:
	SelectLimitOffsetCT(MySQLPtr source) : IConversionTree(source) {}
	virtual bool setup() override;
	virtual void convert(CHPtr & ch_tree) const override;
private:
	int offset;
};

class SelectLimitLengthCT : public IConversionTree
{
public:
	SelectLimitLengthCT(MySQLPtr source) : IConversionTree(source) {}
	virtual bool setup() override;
	virtual void convert(CHPtr & ch_tree) const override;
private:
	int length;
};

class SelectTablesCT : public IConversionTree
{
public:
	struct TableAndDB
	{
		String table = "";
		String database = "";
	};
	SelectTablesCT(MySQLPtr source) : IConversionTree(source) {}
	virtual bool setup() override;
	virtual void convert(CHPtr & ch_tree) const override;
private:
	std::vector<TableAndDB> tables;
};

class SelectQueryCT : public IConversionTree
{
public:
	SelectQueryCT(MySQLPtr source) : IConversionTree(source) {}
	virtual bool setup() override;
	virtual void convert(CHPtr & ch_tree) const override;
private:
	ConvPtr select_items_ct = nullptr;
	ConvPtr order_by_ct = nullptr;
	ConvPtr limit_length_ct = nullptr;
	ConvPtr limit_offset_ct = nullptr;
	ConvPtr tables_ct = nullptr;
	// bool has_limit = false;
	// LimitOption limit_arg; // TODO: int is ok?
	// std::vector<String> tables;
};

}
