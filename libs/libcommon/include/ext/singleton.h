#pragma once


namespace ext
{

/** Example (1):
  *
  *    class Derived : public ext::singleton<Derived>
  *    {
  *        friend class ext::singleton<Derived>;
  *        ...
  *    protected:
  *        Derived() {};
  *    };
  *
  * Example (2):
  *
  *    class Some
  *    {
  *        ...
  *    };
  *
  *    class SomeSingleton : public Some, public ext::singleton<SomeSingleton> {}
  */
template <typename T> class singleton
{
public:
    static T & instance()
    {
        /// C++11 has thread safe statics. GCC and Clang have thread safe statics by default even before C++11.
        static T instance;
        return instance;
    }

protected:
    singleton() {}

private:
    singleton(const singleton &);
    singleton & operator=(const singleton &);
};

}
