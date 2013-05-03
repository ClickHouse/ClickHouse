#pragma once

#include <boost/noncopyable.hpp>

#include <Poco/SharedPtr.h>

#include <DB/Core/Block.h>
#include <DB/Storages/StoragePtr.h>


namespace DB
{

using Poco::SharedPtr;

/** Коллбэк для отслеживания прогресса выполнения запроса.
  * Используется в IProfilingBlockInputStream и Context-е.
  * Функция принимает количество строк в последнем блоке, количество байт в последнем блоке.
  * Следует иметь ввиду, что колбэк может вызываться из разных потоков.
  */
typedef boost::function<void(size_t, size_t)> ProgressCallback;


/** Интерфейс потока для чтения данных по блокам из БД.
  * Реляционные операции предполагается делать также реализациями этого интерфейса.
  */
class IBlockInputStream : private boost::noncopyable
{
public:
	typedef SharedPtr<IBlockInputStream> BlockInputStreamPtr;
	typedef std::vector<BlockInputStreamPtr> BlockInputStreams;
	
	/** Листовой BlockInputStream обычно требует, чтобы был жив какой-то Storage.
	  * Переданный сюда указатель на Storage будет просто храниться в этом экземпляре,
	  *  не позволяя уничтожить Storage раньше этого BlockInputStream.
	  */
	IBlockInputStream(StoragePtr owned_storage_ = StoragePtr()) : owned_storage(owned_storage_) {}
	
	/** Прочитать следующий блок.
	  * Если блоков больше нет - вернуть пустой блок (для которого operator bool возвращает false).
	  */
	virtual Block read() = 0;

	/** Прочитать что-нибудь перед началом всех данных или после конца всех данных.
	  */
	virtual void readPrefix() {}
	virtual void readSuffix() {}

	virtual ~IBlockInputStream() {}

	/** Для вывода дерева преобразований потока данных (плана выполнения запроса).
	  */
	virtual String getName() const = 0;
	virtual String getShortName() const;	/// То же самое, но без BlockInputStream на конце.

	/** Создать копию объекта.
	  * Предполагается, что функция вызывается только до использования объекта (сразу после создания, до вызова других методов),
	  *  только для того, чтобы можно было преобразовать параметр, переданный по ссылке в shared ptr.
	  */
	virtual BlockInputStreamPtr clone() = 0;

	/** Уникальный идентификатор части конвейера выполнения запроса.
	  * Источники с одинаковым идентификатором считаются идентичными
	  *  (выдающими одинаковые данные), и могут быть заменены на один источник
	  *  при одновременном выполнении сразу нескольких запросов.
	  * Если источник нельзя склеивать ни с каким другим - верните в качестве идентификатора адрес объекта.
	  */
	virtual String getID() const = 0;

	BlockInputStreams & getChildren() { return children; }
	
	void dumpTree(std::ostream & ostr, size_t indent = 0, size_t multiplier = 1);
	void dumpTreeWithProfile(std::ostream & ostr, size_t indent = 0);

	/// Получить листовые источники (не считая этот).
	BlockInputStreams getLeaves();

	/// Получить количество строк и байт, прочитанных в листовых источниках.
	void getLeafRowsBytes(size_t & rows, size_t & bytes);

	/** Получить текст, который идентифицирует этот источник и всё поддерево.
	  * Обычно он содержит идентификатор источника и getTreeID от всех детей.
	  */
	String getTreeID() const;

	/** Проверить глубину конвейера.
	  * Если задано max_depth и глубина больше - кинуть исключение.
	  */
	size_t checkDepth(size_t max_depth) const;

protected:
	BlockInputStreams children;
	
	StoragePtr owned_storage;

private:
	void getLeavesImpl(BlockInputStreams & res, BlockInputStreamPtr this_shared_ptr = NULL);

	size_t checkDepthImpl(size_t max_depth, size_t level) const;
};


typedef IBlockInputStream::BlockInputStreamPtr BlockInputStreamPtr;
typedef IBlockInputStream::BlockInputStreams BlockInputStreams;

}

