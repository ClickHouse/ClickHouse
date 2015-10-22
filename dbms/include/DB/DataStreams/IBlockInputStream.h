#pragma once

#include <boost/noncopyable.hpp>

#include <Poco/SharedPtr.h>

#include <DB/Core/Block.h>
#include <DB/Core/Progress.h>
#include <DB/Storages/IStorage.h>


namespace DB
{

using Poco::SharedPtr;

/** Коллбэк для отслеживания прогресса выполнения запроса.
  * Используется в IProfilingBlockInputStream и Context-е.
  * Функция принимает количество строк в последнем блоке, количество байт в последнем блоке.
  * Следует иметь ввиду, что колбэк может вызываться из разных потоков.
  */
typedef std::function<void(const Progress & progress)> ProgressCallback;


/** Интерфейс потока для чтения данных по блокам из БД.
  * Реляционные операции предполагается делать также реализациями этого интерфейса.
  */
class IBlockInputStream : private boost::noncopyable
{
public:
	typedef SharedPtr<IBlockInputStream> BlockInputStreamPtr;
	typedef std::vector<BlockInputStreamPtr> BlockInputStreams;

	IBlockInputStream() {}

	/** Прочитать следующий блок.
	  * Если блоков больше нет - вернуть пустой блок (для которого operator bool возвращает false).
	  */
	virtual Block read() = 0;

	/** Получить информацию про последний полученный блок.
	  */
	virtual BlockExtraInfo getBlockExtraInfo() const
	{
		throw Exception("Method getBlockExtraInfo is not supported by the data stream " + getName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	/** Прочитать что-нибудь перед началом всех данных или после конца всех данных.
	  * В функции readSuffix можно реализовать финализацию, которая может привести к исключению.
	  * readPrefix() должна вызываться до первого вызова read().
	  * readSuffix() должна вызываться после того, как read() вернула пустой блок, или после вызова cancel(), но не во время выполнения read().
	  */
	virtual void readPrefix() {}
	virtual void readSuffix() {}

	virtual ~IBlockInputStream() {}

	/** Для вывода дерева преобразований потока данных (плана выполнения запроса).
	  */
	virtual String getName() const = 0;

	/** Уникальный идентификатор части конвейера выполнения запроса.
	  * Источники с одинаковым идентификатором считаются идентичными
	  *  (выдающими одинаковые данные), и могут быть заменены на один источник
	  *  при одновременном выполнении сразу нескольких запросов.
	  * Если источник нельзя склеивать ни с каким другим - верните в качестве идентификатора адрес объекта.
	  */
	virtual String getID() const = 0;

	BlockInputStreams & getChildren() { return children; }

	void dumpTree(std::ostream & ostr, size_t indent = 0, size_t multiplier = 1);

	/// Получить листовые источники (не считая этот).
	BlockInputStreams getLeaves();

	/// Получить количество строк и байт, прочитанных в листовых источниках.
	void getLeafRowsBytes(size_t & rows, size_t & bytes);

	/** Проверить глубину конвейера.
	  * Если задано max_depth и глубина больше - кинуть исключение.
	  */
	size_t checkDepth(size_t max_depth) const;

	/** Не давать изменить таблицу, пока жив поток блоков.
	  */
	void addTableLock(const IStorage::TableStructureReadLockPtr & lock) { table_locks.push_back(lock); }

protected:
	IStorage::TableStructureReadLocks table_locks;

	BlockInputStreams children;

private:
	void getLeavesImpl(BlockInputStreams & res, BlockInputStreamPtr this_shared_ptr = nullptr);

	size_t checkDepthImpl(size_t max_depth, size_t level) const;

	/** Получить текст, который идентифицирует этот источник и всё поддерево.
	  * В отличие от getID - без учёта параметров.
	  */
	String getTreeID() const;
};


}

