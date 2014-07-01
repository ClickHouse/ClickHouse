#pragma once
#include <zkutil/Types.h>
#include <zkutil/KeeperException.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <unordered_set>


namespace zkutil
{

const UInt32 DEFAULT_SESSION_TIMEOUT = 30000;

struct WatchWithEvent;

/// Из-за вызова С кода легче самому явно управлять памятью
typedef WatchWithEvent * WatchWithEventPtr;

/** Сессия в ZooKeeper. Интерфейс существенно отличается от обычного API ZooKeeper.
  * Вместо callback-ов для watch-ей используются Poco::Event. Для указанного события вызывается set() только при первом вызове watch.
  * Методы с названиями, не начинающимися с try, бросают исключение при любой ошибке кроме OperationTimeout.
  * При OperationTimeout пытаемся попробоватть еще retry_num раз.
  * Методы с названиями, начинающимися с try, не бросают исключение только при перечисленных видах ошибок.
  * Например, исключение бросается в любом случае, если сессия разорвалась или если не хватает прав или ресурсов.
  */
class ZooKeeper
{
public:
	typedef Poco::SharedPtr<ZooKeeper> Ptr;

	ZooKeeper(const std::string & hosts, int32_t sessionTimeoutMs = DEFAULT_SESSION_TIMEOUT, WatchFunction * watch = nullptr);

	/** конфиг вида
		<zookeeper>
			<node>
				<host>example1</host>
				<port>2181</port>
			</node>
			<node>
				<host>example2</host>
				<port>2181</port>
			</node>
			<session_timeout_ms>30000</session_timeout_ms>
		</zookeeper>
	*/
	ZooKeeper(const Poco::Util::AbstractConfiguration & config, const std::string & config_name,
			  WatchFunction * watch = nullptr);

	~ZooKeeper();

	/** Создает новую сессию с теми же параметрами. Можно использовать для переподключения, если сессия истекла.
	  * Новой сессии соответствует только возвращенный экземпляр ZooKeeper, этот экземпляр не изменяется.
	  */
	Ptr startNewSession() const;

	int state();

	/** Возвращает true, если сессия навсегда завершена.
	  * Это возможно только если соединение было установлено, потом разорвалось, потом снова восстановилось, но слишком поздно.
	  * Это достаточно редкая ситуация.
	  * С другой стороны, если, например, указан неправильный сервер или порт, попытки соединения будут продолжаться бесконечно,
	  *  expired() будет возвращать false, и все вызовы будут выбрасывать исключение ConnectionLoss.
	  */
	bool expired();

	AclPtr getDefaultACL();

	void setDefaultACL(AclPtr new_acl);

	/** Создать znode. Используется ACL, выставленный вызовом setDefaultACL (по умолчанию, всем полный доступ).
	  * Если что-то пошло не так, бросить исключение.
	  */
	std::string create(const std::string & path, const std::string & data, int32_t mode);

	/** Не бросает исключение при следующих ошибках:
	  *  - Нет родителя создаваемой ноды.
	  *  - Родитель эфемерный.
	  *  - Такая нода уже есть.
	  * При остальных ошибках бросает исключение.
	  */
	int32_t tryCreate(const std::string & path, const std::string & data, int32_t mode, std::string & pathCreated);
	int32_t tryCreate(const std::string & path, const std::string & data, int32_t mode);

	/** Удалить ноду, если ее версия равна version (если -1, подойдет любая версия).
	  */
	void remove(const std::string & path, int32_t version = -1);

	/** Не бросает исключение при следующих ошибках:
	  *  - Такой ноды нет.
	  *  - У ноды другая версия.
	  *  - У ноды есть дети.
	  */
	int32_t tryRemove(const std::string & path, int32_t version = -1);

	bool exists(const std::string & path, Stat * stat = nullptr, EventPtr watch = nullptr);

	std::string get(const std::string & path, Stat * stat = nullptr, EventPtr watch = nullptr);

	/** Не бросает исключение при следующих ошибках:
	  *  - Такой ноды нет. В таком случае возвращает false.
	  */
	bool tryGet(const std::string & path, std::string & res, Stat * stat = nullptr, EventPtr watch = nullptr);

	void set(const std::string & path, const std::string & data,
			int32_t version = -1, Stat * stat = nullptr);

	/** Не бросает исключение при следующих ошибках:
	  *  - Такой ноды нет.
	  *  - У ноды другая версия.
	  */
	int32_t trySet(const std::string & path, const std::string & data,
							int32_t version = -1, Stat * stat = nullptr);

	Strings getChildren(const std::string & path,
						Stat * stat = nullptr,
						EventPtr watch = nullptr);

	/** Не бросает исключение при следующих ошибках:
	  *  - Такой ноды нет.
	  */
	int32_t tryGetChildren(const std::string & path, Strings & res,
						Stat * stat = nullptr,
						EventPtr watch = nullptr);

	/** Транзакционно выполняет несколько операций. При любой ошибке бросает исключение.
	  */
	OpResultsPtr multi(const Ops & ops);

	/** Бросает исключение только если какая-нибудь операция вернула "неожиданную" ошибку - такую ошибку,
	  *  увидев которую соответствующий метод try* бросил бы исключение. */
	int32_t tryMulti(const Ops & ops, OpResultsPtr * out_results = nullptr);


	/** Удаляет ноду вместе с поддеревом. Если в это время кто-то добавит иили удалит ноду в поддереве, результат не определен.
	  */
	void removeRecursive(const std::string & path);

	static std::string error2string(int32_t code);

	/// максимальный размер данных в узле в байтах
	/// В версии 3.4.5. максимальный размер узла 1 Mb
	static const size_t MAX_NODE_SIZE = 1048576;

	/// Размер прибавляемого ZooKeeper суффикса при создании Sequential ноды
	/// На самом деле размер меньше, но для удобства округлим в верхнюю сторону
	static const size_t SEQUENTIAL_SUFFIX_SIZE = 64;
private:
	friend struct WatchWithEvent;

	void init(const std::string & hosts, int32_t sessionTimeoutMs, WatchFunction * watch_);
	void removeChildrenRecursive(const std::string & path);
	void * watchForEvent(EventPtr event);
	watcher_fn callbackForEvent(EventPtr event);
	static void processEvent(zhandle_t * zh, int type, int state, const char * path, void *watcherCtx);

	template <class T>
	int32_t retry(const T & operation)
	{
		int32_t code = operation();
		for (size_t i = 0; (i < retry_num) && (code == ZOPERATIONTIMEOUT); ++i)
		{
			code = operation();
		}
		return code;
	}

	/// методы не бросают исключений, а возвращают коды ошибок
	int32_t createImpl(const std::string & path, const std::string & data, int32_t mode, std::string & pathCreated);
	int32_t removeImpl(const std::string & path, int32_t version = -1);
	int32_t getImpl(const std::string & path, std::string & res, Stat * stat = nullptr, EventPtr watch = nullptr);
	int32_t setImpl(const std::string & path, const std::string & data,
							int32_t version = -1, Stat * stat = nullptr);
	int32_t getChildrenImpl(const std::string & path, Strings & res,
						Stat * stat = nullptr,
						EventPtr watch = nullptr);
	int32_t multiImpl(const Ops & ops, OpResultsPtr * out_results = nullptr);
	int32_t existsImpl(const std::string & path, Stat * stat_, EventPtr watch = nullptr);

	std::string hosts;
	int32_t sessionTimeoutMs;

	Poco::FastMutex mutex;
	AclPtr default_acl;
	zhandle_t * impl;

	WatchFunction * state_watch;
	std::unordered_set<WatchWithEvent *> watch_store;

	/// Количество попыток повторить операцию при OperationTimeout
	size_t retry_num = 3;
};

typedef ZooKeeper::Ptr ZooKeeperPtr;


/** В конструкторе создает эфемерную ноду, в деструкторе - удаляет.
  */
class EphemeralNodeHolder
{
public:
	typedef Poco::SharedPtr<EphemeralNodeHolder> Ptr;

	EphemeralNodeHolder(const std::string & path_, ZooKeeper & zookeeper_, bool create, bool sequential, const std::string & data)
		: path(path_), zookeeper(zookeeper_)
	{
		if (create)
			path = zookeeper.create(path, data, sequential ? CreateMode::EphemeralSequential : CreateMode::Ephemeral);
	}

	std::string getPath() const
	{
		return path;
	}

	static Ptr create(const std::string & path, ZooKeeper & zookeeper, const std::string & data = "")
	{
		return new EphemeralNodeHolder(path, zookeeper, true, false, data);
	}

	static Ptr createSequential(const std::string & path, ZooKeeper & zookeeper, const std::string & data = "")
	{
		return new EphemeralNodeHolder(path, zookeeper, true, true, data);
	}

	static Ptr existing(const std::string & path, ZooKeeper & zookeeper)
	{
		return new EphemeralNodeHolder(path, zookeeper, false, false, "");
	}

	~EphemeralNodeHolder()
	{
		try
		{
			zookeeper.tryRemove(path);
		}
		catch (KeeperException) {}
	}

private:
	std::string path;
	ZooKeeper & zookeeper;
};

typedef EphemeralNodeHolder::Ptr EphemeralNodeHolderPtr;

}
