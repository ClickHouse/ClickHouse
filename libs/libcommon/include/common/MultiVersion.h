#pragma once

#include <mutex>
#include <memory>


/** Позволяет хранить некоторый объект, использовать его read-only в разных потоках,
  *  и заменять его на другой в других потоках.
  * Замена производится атомарно, при этом, читающие потоки могут работать с разными версиями объекта.
  *
  * Использование:
  *        MultiVersion<T> x;
  * - при обновлении данных:
  *        x.set(new value);
  * - при использовании данных для чтения в разных потоках:
  *    {
  *        MultiVersion<T>::Version current_version = x.get();
  *        // используем для чего-нибудь *current_version
  * }    // здесь перестаём владеть версией; если версия устарела, и её никто больше не использует - она будет уничтожена
  *
  * Все методы thread-safe.
  */
template <typename T, typename Ptr = std::shared_ptr<T>>
class MultiVersion
{
public:
    /// Конкретная версия объекта для использования. shared_ptr определяет время жизни версии.
    using Version = Ptr;

    /// Инициализация по-умолчанию (NULL-ом).
    MultiVersion() = default;

    /// Инициализация первой версией.
    MultiVersion(const Version & value)
    {
        set(value);
    }

    /// Захватить владение первой версией.
    MultiVersion(T * value)
    {
        set(value);
    }

    MultiVersion(Version && value)
    {
        set(std::move(value));
    }

    MultiVersion(std::unique_ptr<T> && value)
    {
        set(std::move(value));
    }

    /// Получить текущую версию для использования. Возвращает shared_ptr, который определяет время жизни версии.
    const Version get() const
    {
        /// TODO: можно ли заменять shared_ptr lock-free? (Можно, если сделать свою реализацию с использованием cmpxchg16b.)
        std::lock_guard<std::mutex> lock(mutex);
        return current_version;
    }

    /// Обновить объект новой версией.
    void set(Version value)
    {
        std::lock_guard<std::mutex> lock(mutex);
        current_version = value;
    }

    /// Обновить объект новой версией и захватить владение.
    void set(T * value)
    {
        set(Version(value));
    }

    void set(std::unique_ptr<T> && value)
    {
        set(Version(value.release()));
    }

private:
    Version current_version;
    mutable std::mutex mutex;
};
