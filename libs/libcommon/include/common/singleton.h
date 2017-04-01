#pragma once

/** Пример:
  *
  *    class Derived : public Singleton<Derived>
  *    {
  *        friend class Singleton<Derived>;
  *     ...
  *    protected:
  *        Derived() {};
  *    };
  *
  * Или так:
  *
  * class Some
  * {
  *     ...
  * };
  *
  * class SomeSingleton : public Some, public Singleton<SomeSingleton> {}
  */
template<class Subject> class Singleton
{
public:
    static Subject & instance()
    {
        /// Нормально при включенных thread safe statics в gcc (по-умолчанию).
        static Subject instance;
        return instance;
    }

protected:
    Singleton(){};

private:
    Singleton(const Singleton&);
    Singleton& operator=(const Singleton&);
};
