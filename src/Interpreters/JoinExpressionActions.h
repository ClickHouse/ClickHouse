#pragma once

#include <Interpreters/ActionsDAG.h>
#include <ranges>
#include <boost/dynamic_bitset.hpp>
#include <Core/Joins.h>
#include <Functions/IFunctionAdaptors.h>

namespace DB
{

enum class JoinConditionOperator : UInt8
{
    And,
    Or,
    Equals,
    NullSafeEquals,
    Less,
    LessOrEquals,
    Greater,
    GreaterOrEquals,
    Unknown,
};

std::string_view toString(JoinConditionOperator op);

class BitSet
{
public:
    BitSet() = default;

    BitSet & set(size_t pos)
    {
        if (pos >= bitset.size())
            bitset.resize(pos + 1);
        bitset.set(pos);
        return *this;
    }

    bool any() const { return bitset.any(); }
    bool none() const { return bitset.none(); }

    friend bool operator ==(const BitSet & lhs, const BitSet & rhs)
    {
        adjustSize(lhs, rhs);
        return lhs.bitset == rhs.bitset;
    }

    friend BitSet operator &(const BitSet & lhs, const BitSet & rhs)
    {
        adjustSize(lhs, rhs);
        return BitSet(lhs.bitset & rhs.bitset);
    }

    friend BitSet operator |(const BitSet & lhs, const BitSet & rhs)
    {
        adjustSize(lhs, rhs);
        return BitSet(lhs.bitset | rhs.bitset);
    }

private:
    using Base = boost::dynamic_bitset<>;

    static void adjustSize(const BitSet & lhs, const BitSet & rhs)
    {
        auto max_size = std::max(lhs.bitset.size(), rhs.bitset.size());
        lhs.bitset.resize(max_size);
        rhs.bitset.resize(max_size);
    }

    explicit BitSet(Base && base) : bitset(std::move(base)) {}

    mutable Base bitset;
};

inline bool isSubsetOf(const BitSet & lhs, const BitSet & rhs) { return (lhs & rhs) == lhs; }

class JoinActionRef;

class JoinExpressionActions
{
public:
    using NodeRawPtr = const ActionsDAG::Node *;

    JoinExpressionActions(const Block & left_header, const Block & right_header);
    JoinExpressionActions(const Block & left_header, const Block & right_header, ActionsDAG && actions_dag);

    JoinExpressionActions clone(std::vector<JoinActionRef> & nodes) const;

    JoinActionRef findNode(const String & column_name, bool is_input = false, bool throw_if_not_found = true) const;

    std::shared_ptr<ActionsDAG> getActionsDAG() const;

    template <std::ranges::range Range>
    requires std::convertible_to<std::ranges::range_value_t<Range>, JoinActionRef>
    static ActionsDAG getSubDAG(Range && range)
    {
        auto nodes = std::ranges::to<std::vector>(range | std::views::transform([](const auto & action) { return action.getNode(); }));
        return ActionsDAG::cloneSubDAG(nodes, /* remove_aliases= */ false);
    }

    static ActionsDAG getSubDAG(JoinActionRef action);

    JoinExpressionActions(const JoinExpressionActions &) = delete;
    JoinExpressionActions & operator=(const JoinExpressionActions &) = delete;

    JoinExpressionActions(JoinExpressionActions &&) = default;
    JoinExpressionActions & operator=(JoinExpressionActions &&) = default;

private:
    friend class JoinActionRef;

    struct Data;
    explicit JoinExpressionActions(std::shared_ptr<Data> data_) : data(data_) {}

    std::shared_ptr<Data> data;
};

class JoinActionRef
{
public:
    using NodeRawPtr = JoinExpressionActions::NodeRawPtr;

    JoinActionRef(std::nullptr_t) : node_ptr(nullptr) {} /// NOLINT

    explicit JoinActionRef(NodeRawPtr node_, JoinExpressionActions & expression_actions_);
    explicit JoinActionRef(NodeRawPtr node_, std::shared_ptr<JoinExpressionActions::Data> data_);

    class AddFunction
    {
    public:
        explicit AddFunction(JoinConditionOperator op);
        explicit AddFunction(FunctionOverloadResolverPtr function_ptr_);
        explicit AddFunction(std::shared_ptr<IFunction> function_);

        NodeRawPtr operator()(ActionsDAG & dag, std::vector<NodeRawPtr> nodes);
    private:
        FunctionOverloadResolverPtr function_ptr;
    };

    template <typename F>
    static JoinActionRef transform(const std::vector<JoinActionRef> & actions, F && func)
    {
        auto data_ptr = getData(actions);
        auto nodes = std::ranges::to<std::vector>(actions | std::views::transform([](const auto & action) { return action.getNode(); }));
        return JoinActionRef(func(getActionsDAG(*data_ptr), std::move(nodes)), data_ptr);
    }

    NodeRawPtr getNode() const;

    ColumnWithTypeAndName getColumn() const;
    const String & getColumnName() const;
    DataTypePtr getType() const;

    operator bool() const { return node_ptr != nullptr; } /// NOLINT

    std::vector<JoinActionRef> getArguments(bool recursive = false) const;

    void setSourceRelations(const BitSet & source_relations) const;
    BitSet getSourceRelations() const;
    bool fromLeft() const;
    bool fromRight() const;
    bool fromNone() const;

    bool isFunction(JoinConditionOperator op) const;
    std::tuple<JoinConditionOperator, JoinActionRef, JoinActionRef> asBinaryPredicate() const;

    friend bool operator==(const JoinActionRef & left, const JoinActionRef & right) { return left.node_ptr == right.node_ptr; }

private:
    std::shared_ptr<JoinExpressionActions::Data> getData() const;
    static std::shared_ptr<JoinExpressionActions::Data> getData(const std::vector<JoinActionRef> & actions);
    static ActionsDAG & getActionsDAG(JoinExpressionActions::Data & data_);

    NodeRawPtr node_ptr = nullptr;
    std::weak_ptr<JoinExpressionActions::Data> data = {};
};

}

template <> struct std::hash<DB::JoinActionRef>
{
    size_t operator()(const DB::JoinActionRef & ref) const { return std::hash<const DB::ActionsDAG::Node *>()(ref.getNode()); }
};
