#pragma once
#include <Processors/Formats/Impl/Parquet/ColumnFilter.h>

namespace DB
{
using NamedColumnFilter = std::pair<String, ColumnFilterPtr>;
class ColumnFilterFactory;
using ColumnFilterFactoryPtr = std::shared_ptr<ColumnFilterFactory>;

class ColumnFilterFactory
{
public:
    static std::vector<ColumnFilterFactoryPtr> allFactories();

    virtual ~ColumnFilterFactory() = 0;
    // Check if the node can be used to create a column filter.
    virtual bool validate(const ActionsDAG::Node & node) = 0;
    // Create a column filter. Should be called after validate.
    virtual NamedColumnFilter create(const ActionsDAG::Node & node) = 0;
};


class BigIntRangeFilterFactory : public ColumnFilterFactory
{
public:
    ~BigIntRangeFilterFactory() override = default;
    bool validate(const ActionsDAG::Node & node) override;
    NamedColumnFilter create(const ActionsDAG::Node & node) override;
};

class NegatedBigIntRangeFilterFactory : public ColumnFilterFactory
{
public:
    ~NegatedBigIntRangeFilterFactory() override = default;
    bool validate(const ActionsDAG::Node & node) override;
    NamedColumnFilter create(const ActionsDAG::Node & node) override;

private:
    BigIntRangeFilterFactory non_negated_factory;
};

template <is_float T>
class FloatRangeFilterFactory : public ColumnFilterFactory
{
public:
    ~FloatRangeFilterFactory() override = default;
    bool validate(const ActionsDAG::Node & node) override;
    NamedColumnFilter create(const ActionsDAG::Node & node) override;
};

class BytesValuesFilterFactory : public ColumnFilterFactory
{
public:
    ~BytesValuesFilterFactory() override = default;
    bool validate(const ActionsDAG::Node & node) override;
    NamedColumnFilter create(const ActionsDAG::Node & node) override;
};

class NegatedBytesValuesFilterFactory : public ColumnFilterFactory
{
public:
    ~NegatedBytesValuesFilterFactory() override = default;
    bool validate(const ActionsDAG::Node & node) override;
    NamedColumnFilter create(const ActionsDAG::Node & node) override;

private:
    BytesValuesFilterFactory non_negated_factory;
};

class IsNullFilterFactory : public ColumnFilterFactory
{
public:
    ~IsNullFilterFactory() override = default;
    bool validate(const ActionsDAG::Node & node) override;
    NamedColumnFilter create(const ActionsDAG::Node & node) override;
};

class IsNotNullFilterFactory : public ColumnFilterFactory
{
public:
    ~IsNotNullFilterFactory() override = default;
    bool validate(const ActionsDAG::Node & node) override;
    NamedColumnFilter create(const ActionsDAG::Node & node) override;

private:
    IsNullFilterFactory non_negated_factory;
};
}
