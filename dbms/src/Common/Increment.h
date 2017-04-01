#pragma once

#include <Common/CounterInFile.h>


/** Позволяет получать авто-инкрементное число, храня его в файле.
  * Предназначен для редких вызовов (не рассчитан на производительность).
  */
class Increment
{
public:
    /// path - имя файла, включая путь
    Increment(const std::string & path_) : counter(path_) {}

    /** Получить следующее число.
      * Если параметр create_if_need не установлен в true, то
      *  в файле уже должно быть записано какое-нибудь число (если нет - создайте файл вручную с нулём).
      *
      * Для защиты от race condition-ов между разными процессами, используются файловые блокировки.
      * (Но при первом создании файла race condition возможен, так что лучше создать файл заранее.)
      *
      * locked_callback вызывается при заблокированном файле со счетчиком. В него передается новое значение.
      * locked_callback можно использовать, чтобы делать что-нибудь атомарно с увеличением счетчика (например, переименовывать файлы).
      */
    template <typename Callback>
    UInt64 get(Callback && locked_callback, bool create_if_need = false)
    {
        return static_cast<UInt64>(counter.add(1, std::forward<Callback>(locked_callback), create_if_need));
    }

    UInt64 get(bool create_if_need = false)
    {
        return getBunch(1, create_if_need);
    }

    /// Посмотреть следующее значение.
    UInt64 peek(bool create_if_need = false)
    {
        return getBunch(0, create_if_need);
    }

    /** Получить следующее число и увеличить счетчик на count.
     * Если параметр create_if_need не установлен в true, то
     *  в файле уже должно быть записано какое-нибудь число (если нет - создайте файл вручную с нулём).
     *
     * Для защиты от race condition-ов между разными процессами, используются файловые блокировки.
     * (Но при первом создании файла race condition возможен, так что лучше создать файл заранее.)
     */
    UInt64 getBunch(UInt64 count, bool create_if_need = false)
    {
        return static_cast<UInt64>(counter.add(static_cast<Int64>(count), create_if_need) - count + 1);
    }

    /// Изменить путь к файлу.
    void setPath(std::string path_)
    {
        counter.setPath(path_);
    }

    void fixIfBroken(UInt64 value)
    {
        counter.fixIfBroken(value);
    }

private:
    CounterInFile counter;
};


/** То же самое, но без хранения в файле.
  */
struct SimpleIncrement : private boost::noncopyable
{
    std::atomic<UInt64> value;

    SimpleIncrement(UInt64 start = 0) : value(start) {}

    void set(UInt64 new_value)
    {
        value = new_value;
    }

    UInt64 get()
    {
        return ++value;
    }
};
