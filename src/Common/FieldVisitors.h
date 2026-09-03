#pragma once

#include <Core/Field.h>


namespace DB
{

/** StaticVisitor (and its descendants) - class with overloaded operator() for all types of fields.
  * You could call visitor for field using function 'applyVisitor'.
  * Also "binary visitor" is supported - its operator() takes two arguments.
  */
template <typename R = void>
struct StaticVisitor
{
    using ResultType = R;
};


/// F is template parameter, to allow universal reference for field, that is useful for const and non-const values.
template <typename Visitor, typename F>
auto applyVisitor(Visitor && visitor, F && field)
{
    return Field::dispatch(std::forward<Visitor>(visitor),
        std::forward<F>(field));
}

template <typename Visitor, typename F1, typename F2>
auto applyVisitor(Visitor && visitor, F1 && field1, F2 && field2)
{
    return Field::dispatch(
        [&field2, &visitor](auto & field1_value)
        {
            return Field::dispatch(
                [&field1_value, &visitor](auto & field2_value)
                {
                    return visitor(field1_value, field2_value);
                },
                std::forward<F2>(field2));
        },
        std::forward<F1>(field1));
}

}
