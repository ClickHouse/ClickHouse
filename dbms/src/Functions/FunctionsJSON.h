#pragma once

#include <Functions/IFunction.h>
#include <Common/config.h>

#if USE_SIMDJSON

#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeFactory.h>
#include <Common/typeid_cast.h>
#include <ext/range.h>

#ifdef __clang__
    #pragma clang diagnostic push
    #pragma clang diagnostic ignored "-Wold-style-cast"
    #pragma clang diagnostic ignored "-Wnewline-eof"
#endif

#include <simdjson/jsonparser.h>

#ifdef __clang__
    #pragma clang diagnostic pop
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

template <typename Impl, bool ExtraArg>
class FunctionJSONBase : public IFunction
{
private:
    enum class Action
    {
        key = 1,
        index = 2,
    };

    mutable std::vector<Action> actions;
    mutable DataTypePtr virtual_type;

    bool tryMove(ParsedJson::iterator & pjh, Action action, const Field & accessor)
    {
        switch (action)
        {
            case Action::key:
                if (!pjh.is_object() || !pjh.move_to_key(accessor.get<String>().data()))
                    return false;

                break;
            case Action::index:
                if (!pjh.is_object_or_array() || !pjh.down())
                    return false;

                int steps = accessor.get<Int64>();

                if (steps > 0)
                    steps -= 1;
                else if (steps < 0)
                {
                    steps += 1;

                    ParsedJson::iterator pjh1{pjh};

                    while (pjh1.next())
                        steps += 1;
                }
                else
                    return false;

                for (const auto i : ext::range(0, steps))
                {
                    (void)i;

                    if (!pjh.next())
                        return false;
                }

                break;
        }

        return true;
    }

public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create(const Context &) { return std::make_shared<FunctionJSONBase>(); }

    String getName() const override { return Impl::name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if constexpr (ExtraArg)
        {
            if (arguments.size() < 2)
                throw Exception{"Function " + getName() + " requires at least two arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

            auto col_type_const = typeid_cast<const ColumnConst *>(arguments[1].column.get());

            if (!col_type_const)
                throw Exception{"Illegal non-const column " + arguments[1].column->getName() + " of argument of function " + getName(),
                                ErrorCodes::ILLEGAL_COLUMN};

            virtual_type = DataTypeFactory::instance().get(col_type_const->getValue<String>());
        }
        else
        {
            if (arguments.size() < 1)
                throw Exception{"Function " + getName() + " requires at least one arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};
        }

        if (!isString(arguments[0].type))
            throw Exception{"Illegal type " + arguments[0].type->getName() + " of argument of function " + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        actions.reserve(arguments.size() - 1 - ExtraArg);

        for (const auto i : ext::range(1 + ExtraArg, arguments.size()))
        {
            if (isString(arguments[i].type))
                actions.push_back(Action::key);
            else if (isInteger(arguments[i].type))
                actions.push_back(Action::index);
            else
                throw Exception{"Illegal type " + arguments[i].type->getName() + " of argument of function " + getName(),
                                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        if constexpr (ExtraArg)
            return Impl::getType(virtual_type);
        else
            return Impl::getType();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result_pos, size_t input_rows_count) override
    {
        MutableColumnPtr to{block.getByPosition(result_pos).type->createColumn()};
        to->reserve(input_rows_count);

        const ColumnPtr & arg_json = block.getByPosition(arguments[0]).column;

        auto col_json_const = typeid_cast<const ColumnConst *>(arg_json.get());

        auto col_json_string
            = typeid_cast<const ColumnString *>(col_json_const ? col_json_const->getDataColumnPtr().get() : arg_json.get());

        if (!col_json_string)
            throw Exception{"Illegal column " + arg_json->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN};

        const ColumnString::Chars & chars = col_json_string->getChars();
        const ColumnString::Offsets & offsets = col_json_string->getOffsets();

        size_t max_size = 1;

        for (const auto i : ext::range(0, input_rows_count))
            if (max_size < offsets[i] - offsets[i - 1] - 1)
                max_size = offsets[i] - offsets[i - 1] - 1;

        ParsedJson pj;
        if (!pj.allocateCapacity(max_size))
            throw Exception{"Can not allocate memory for " + std::to_string(max_size) + " units when parsing JSON",
                            ErrorCodes::CANNOT_ALLOCATE_MEMORY};

        for (const auto i : ext::range(0, input_rows_count))
        {
            bool ok = json_parse(&chars[offsets[i - 1]], offsets[i] - offsets[i - 1] - 1, pj) == 0;

            ParsedJson::iterator pjh{pj};

            for (const auto j : ext::range(0, actions.size()))
            {
                if (!ok)
                    break;

                ok = tryMove(pjh, actions[j], (*block.getByPosition(arguments[j + 1 + ExtraArg]).column)[i]);
            }

            if (ok)
            {
                if constexpr (ExtraArg)
                    to->insert(Impl::getValue(pjh, virtual_type));
                else
                    to->insert(Impl::getValue(pjh));
            }
            else
            {
                if constexpr (ExtraArg)
                    to->insert(Impl::getDefault(virtual_type));
                else
                    to->insert(Impl::getDefault());
            }
        }

        block.getByPosition(result_pos).column = std::move(to);
    }
};
}
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

template <typename Impl>
class FunctionJSONDummy : public IFunction
{
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionJSONDummy>(); }

    String getName() const override { return Impl::name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName &) const override
    {
        throw Exception{"Function " + getName() + " is not supported without AVX2", ErrorCodes::NOT_IMPLEMENTED};
    }

    void executeImpl(Block &, const ColumnNumbers &, size_t, size_t) override
    {
        throw Exception{"Function " + getName() + " is not supported without AVX2", ErrorCodes::NOT_IMPLEMENTED};
    }
};

}
