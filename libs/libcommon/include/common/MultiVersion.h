#pragma once

#include <Poco/Mutex.h>
#include <Poco/SharedPtr.h>


/** Позволяет хранить некоторый объект, использовать его read-only в разных потоках,
  *  и заменять его на другой в других потоках.
  * Замена производится атомарно, при этом, читающие потоки могут работать с разными версиями объекта.
  *
  * Использование:
  *		MultiVersion<T> x;
  * - при обновлении данных:
  *		x.set(new value);
  * - при использовании данных для чтения в разных потоках:
  *	{
  *		MultiVersion<T>::Version current_version = x.get();
  *		// используем для чего-нибудь *current_version
  * }	// здесь перестаём владеть версией; если версия устарела, и её никто больше не использует - она будет уничтожена
  *
  * Все методы thread-safe.
  */
template <typename T, typename Ptr = Poco::SharedPtr<T> >
class MultiVersion
{
public:
	/// Конкретная версия объекта для использования. SharedPtr определяет время жизни версии.
	typedef Ptr Version;

	/// Инициализация по-умолчанию (NULL-ом).
	MultiVersion() = default;

	/// Инициализация первой версией.
	MultiVersion(const Version & value)
	{
		set(value);
	}

	MultiVersion(Version && value)
	{
		set(std::move(value));
	}

	/// Получить текущую версию для использования. Возвращает SharedPtr, который определяет время жизни версии.
	const Version get() const
	{
		/// TODO: можно ли заменять SharedPtr lock-free? (Можно, если сделать свою реализацию с использованием cmpxchg16b.)
		Poco::ScopedLock<Poco::FastMutex> lock(mutex);
		return current_version;
	}

	/// Обновить объект новой версией.
	void set(Version value)
	{
		Poco::ScopedLock<Poco::FastMutex> lock(mutex);
		current_version = value;
	}
	
private:
	Version current_version;
	mutable Poco::FastMutex mutex;
};
