#pragma once
#include <memory>
#include <stdexcept>
#include <type_traits>

#include <base/TypeList.h>
#include <base/overloaded.h>

/// Helpers to copy reference and topmost cv-qualifiers of From(or referenced by From) to To
template <typename From, typename To>
struct copy_cvref
{
private:
    using removed = std::remove_cvref_t<To>;
    using cv_qual = std::remove_reference_t<From>;
    using cqual = std::conditional_t<std::is_const_v<cv_qual>, std::add_const_t<removed>, removed>;
    using vqual = std::conditional_t<std::is_volatile_v<cv_qual>, std::add_volatile_t<cqual>, cqual>;
    using lref = std::conditional_t<std::is_lvalue_reference_v<From>, std::add_lvalue_reference_t<vqual>, vqual>;
    using rref = std::conditional_t<std::is_rvalue_reference_v<From>, std::add_rvalue_reference_t<lref>, lref>;

public:
    using type = rref;
};

template <typename From, typename To>
using copy_cvref_t = typename copy_cvref<From, To>::type;


/** General factory class for creating std::variant containing pointers to polymorphic classes from the TypeList.
  * The initial value of the variant is a pointer to which the passed base class pointer(or reference)
  * could be casted to dynamically. CV-qualifiers of an input pointer are preserved.
  *
  * The intention is to simplify the use of the std::visit function with polymorphic type hierarchies.
  *
  *
  * Example:
  *
  * struct Base { virtual ~Base() = default; };
  * struct Derived1: public Base {};
  * struct Derived2: public Base {};
  *
  * using DerivedTypeList = TypeList<Derived1, Derived2>;
  * using ExpectedVariant = std::variant<const Derived1 *, const Derived2 *>;
  *
  * const Base* p = new Derived2();
  * auto v = PolymorhicVariantFactory<DerivedTypeList>::AsVariant(p);
  *
  * static_assert(std::is_same_v<ExpectedVariant, decltype(v)>);
  * assert(std::holds_alternative<const Derived2 *>(v) == true);
  *
  * std::visit(overload{
  *      [](const Derived1*) { ... },
  *      [](const Derived2*) { ... },
  *      [](const Base*) { ... }, // Fallback
  * }, v);
  *
  *
  * Or for multiple variants:
  * std::visit(overload{
  *      [](const A1 *, const B1 *) { ... },
  *      [](const A2 *, const B2 *) { ... },
  *      [](const BaseA *, const BaseB *) { ... }, // Fallback
  * }, v1, v2);
  *
  *
*/

template <typename T>
struct PolymorhicVariantFactory
{
};

template <typename... Ts>
struct PolymorhicVariantFactory<TypeList<Ts...>>
{
    template <typename B, typename T>
    using ItemType = std::add_pointer_t<copy_cvref_t<B, T>>;

    template <typename B>
    using VariantType = std::variant<ItemType<B, Ts>...>;

    template <typename BaseT>
    requires(std::is_polymorphic_v<BaseT>)
    static VariantType<BaseT> AsVariant(BaseT * base)
    {
        VariantType<BaseT> ret;

        // For each type in the type list
        (
            [&]
                {
                    if (auto p = dynamic_cast<ItemType<BaseT, Ts>>(base))
                    {
                        ret = p;
                        return true; // short-circuit stop
                    }
                    return false;
                }()
                || ... || []() -> bool { throw std::runtime_error("Not found derived class in the type list"); }());

        return ret;
    }

    template <typename BaseT>
    requires(!std::is_rvalue_reference_v<BaseT &&>)
    static VariantType<BaseT> AsVariant(BaseT && base)
    {
        return AsVariant(std::addressof(base));
    }
};
