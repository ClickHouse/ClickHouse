#pragma once

#include <zkutil/Types.h>
#include <zkutil/KeeperException.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <unordered_set>
#include <future>
#include <memory>
#include <common/logger_useful.h>


namespace zkutil
{

const UInt32 DEFAULT_SESSION_TIMEOUT = 30000;
const UInt32 MEDIUM_SESSION_TIMEOUT = 120000;
const UInt32 BIG_SESSION_TIMEOUT = 600000;
const UInt32 DEFAULT_RETRY_NUM = 3;

struct WatchWithEvent;

/** Сессия в ZooKeeper. Интерфейс существенно отличается от обычного API ZooKeeper.
  * Вместо callback-ов для watch-ей используются Poco::Event. Для указанного события вызывается set() только при первом вызове watch.
  * Методы на чтение при восстанавливаемых ошибках OperationTimeout, ConnectionLoss пытаются еще retry_num раз.
  * Методы на запись не пытаются повторить при восстанавливаемых ошибках, т.к. это приводит к проблеммам типа удаления дважды одного и того же.
  *
  * Методы с названиями, не начинающимися с try, бросают исключение при любой ошибке.
  */
class ZooKeeper
{
public:
	typedef std::shared_ptr<ZooKeeper> Ptr;

	ZooKeeper(const std::string & hosts, int32_t session_timeout_ms = DEFAULT_SESSION_TIMEOUT);

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
	ZooKeeper(const Poco::Util::AbstractConfiguration & config, const std::string & config_name);

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

	ACLPtr getDefaultACL();

	void setDefaultACL(ACLPtr new_acl);

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
	int32_t tryCreateWithRetries(const std::string & path, const std::string & data, int32_t mode,
								 std::string & pathCreated, size_t * attempt = nullptr);

	/** создает Persistent ноду.
	 *  Игнорирует, если нода уже создана.
	 *  Пытается сделать retry при ConnectionLoss или OperationTimeout
	 */
	void createIfNotExists(const std::string & path, const std::string & data);

	/** Создает всех еще не существующих предков ноды, с пустыми данными. Саму указанную ноду не создает.
	  */
	void createAncestors(const std::string & path);

	/** Удалить ноду, если ее версия равна version (если -1, подойдет любая версия).
	  */
	void remove(const std::string & path, int32_t version = -1);

	/** Удаляет ноду. В случае сетевых ошибок пробует удалять повторно.
	 *  Ошибка ZNONODE для второй и последующих попыток игнорируется
	 */
	void removeWithRetries(const std::string & path, int32_t version = -1);

	/** Не бросает исключение при следующих ошибках:
	  *  - Такой ноды нет.
	  *  - У ноды другая версия.
	  *  - У ноды есть дети.
	  */
	int32_t tryRemove(const std::string & path, int32_t version = -1);
	/// Если есть проблемы с сетью может сам удалить ноду и вернуть ZNONODE
	int32_t tryRemoveWithRetries(const std::string & path, int32_t version = -1, size_t * attempt = nullptr);

	bool exists(const std::string & path, Stat * stat = nullptr, EventPtr watch = nullptr);

	std::string get(const std::string & path, Stat * stat = nullptr, EventPtr watch = nullptr);

	/** Не бросает исключение при следующих ошибках:
	  *  - Такой ноды нет. В таком случае возвращает false.
	  */
	bool tryGet(const std::string & path, std::string & res, Stat * stat = nullptr, EventPtr watch = nullptr, int * code = nullptr);

	void set(const std::string & path, const std::string & data,
			int32_t version = -1, Stat * stat = nullptr);

	/** Создает ноду, если ее не существует. Иначе обновляет */
	void createOrUpdate(const std::string & path, const std::string & data, int32_t mode);

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
	/** Использовать только для методов на чтение */
	int32_t tryMultiWithRetries(const Ops & ops, OpResultsPtr * out_results = nullptr, size_t * attempt = nullptr);

	int64_t getClientID();

	/** Удаляет ноду вместе с поддеревом. Если в это время кто-то добавит иили удалит ноду в поддереве, результат не определен.
	  */
	void removeRecursive(const std::string & path);

	/** Удаляет ноду вместе с поддеревом. Если в это время кто-то будет тоже удалять какие-нибудь ноды в поддереве, не будет ошибок.
	  * Например, можно вызвать одновременно дважды для одной ноды, и результат будет тот же, как если вызвать один раз.
	  */
	void tryRemoveRecursive(const std::string & path);

	/** Подождать, пока нода перестанет существовать или вернуть сразу, если нода не существует.
	  */
	void waitForDisappear(const std::string & path);


	/** Асинхронный интерфейс (реализовано небольшое подмножество операций).
	  *
	  * Использование:
	  *
	  * // Эти вызовы не блокируются.
	  * auto future1 = zk.asyncGet("/path1");
	  * auto future2 = zk.asyncGet("/path2");
	  * ...
	  *
	  * // Эти вызовы могут заблокироваться до выполнения операции.
	  * auto result1 = future1.get();
	  * auto result2 = future2.get();
	  *
	  * future не должна быть уничтожена до получения результата.
	  * Результат обязательно необходимо получать.
	  */

	template <typename Result, typename... TaskParams>
	class Future
	{
	friend class ZooKeeper;
	private:
		using Task = std::packaged_task<Result (TaskParams...)>;
		using TaskPtr = std::unique_ptr<Task>;
		using TaskPtrPtr = std::unique_ptr<TaskPtr>;

		/** Всё очень сложно.
		  *
		  * В асинхронном интерфейсе libzookeeper, функция (например, zoo_aget)
		  *  принимает указатель на свободную функцию-коллбэк и void* указатель на данные.
		  * Указатель на данные потом передаётся в callback.
		  * Это значит, что мы должны сами обеспечить, чтобы данные жили во время работы этой функции и до конца работы callback-а,
		  *  и не можем просто так передать владение данными внутрь функции.
		  * Для этого, мы засовываем данные в объект Future, который возвращается пользователю. Данные будут жить, пока живёт объект Future.
		  * Данные засунуты в unique_ptr, чтобы при возврате объекта Future из функции, их адрес (который передаётся в libzookeeper) не менялся.
		  *
		  * Вторая проблема состоит в том, что после того, как std::promise был удовлетворён, и пользователь получил результат из std::future,
		  *  объект Future может быть уничтожен, при чём раньше, чем завершит работу в другом потоке функция, которая удовлетворяет promise.
		  * См. http://stackoverflow.com/questions/10843304/race-condition-in-pthread-once
		  * Чтобы этого избежать, используется второй unique_ptr. Внутри callback-а, void* данные преобразуются в unique_ptr, и
		  *  перемещаются в локальную переменную unique_ptr, чтобы продлить время жизни данных.
		  */

		TaskPtrPtr task;
		std::future<Result> future;

		template <typename... Args>
		Future(Args &&... args) : task(new TaskPtr(new Task(std::forward<Args>(args)...))), future((*task)->get_future()) {}

	public:
		Result get()
		{
			return future.get();
		}

		Future(Future &&) = default;
		Future & operator= (Future &&) = default;

		~Future()
		{
			/** Если никто не дождался результата, то мы должны его дождаться перед уничтожением объекта,
			  *  так как данные этого объекта могут всё ещё использоваться в колбэке.
			  */
			if (future.valid())
				future.wait();
		}
	};


	struct ValueAndStat
	{
		std::string value;
		Stat stat;
	};

	using GetFuture = Future<ValueAndStat, int, const char *, int, const Stat *>;
	GetFuture asyncGet(const std::string & path);


	struct ValueAndStatAndExists
	{
		std::string value;
		Stat stat;
		bool exists;
	};

	using TryGetFuture = Future<ValueAndStatAndExists, int, const char *, int, const Stat *>;
	TryGetFuture asyncTryGet(const std::string & path);


	struct StatAndExists
	{
		Stat stat;
		bool exists;
	};

	using ExistsFuture = Future<StatAndExists, int, const Stat *>;
	ExistsFuture asyncExists(const std::string & path);


	using GetChildrenFuture = Future<Strings, int, const String_vector *>;
	GetChildrenFuture asyncGetChildren(const std::string & path);


	using RemoveFuture = Future<void, int>;
	RemoveFuture asyncRemove(const std::string & path);


	static std::string error2string(int32_t code);

	/// максимальный размер данных в узле в байтах
	/// В версии 3.4.5. максимальный размер узла 1 Mb
	static const size_t MAX_NODE_SIZE = 1048576;

	/// Размер прибавляемого ZooKeeper суффикса при создании Sequential ноды
	/// На самом деле размер меньше, но для удобства округлим в верхнюю сторону
	static const size_t SEQUENTIAL_SUFFIX_SIZE = 64;


	zhandle_t * getHandle() { return impl; }

private:
	friend struct WatchWithEvent;
	friend class EphemeralNodeHolder;

	void init(const std::string & hosts, int32_t session_timeout_ms);
	void removeChildrenRecursive(const std::string & path);
	void tryRemoveChildrenRecursive(const std::string & path);
	void * watchForEvent(EventPtr event);
	watcher_fn callbackForEvent(EventPtr event);
	static void processEvent(zhandle_t * zh, int type, int state, const char * path, void * watcherCtx);

	template <class T>
	int32_t retry(T && operation, size_t * attempt = nullptr)
	{
		int32_t code = operation();
		if (attempt)
			*attempt = 0;
		for (size_t i = 0; (i < retry_num) && (code == ZOPERATIONTIMEOUT || code == ZCONNECTIONLOSS); ++i)
		{
			if (attempt)
				*attempt = i;

			/// если потеряно соединение подождем timeout/3, авось восстановится
			static const int MAX_SLEEP_TIME = 10;
			if (code == ZCONNECTIONLOSS)
				usleep(std::min(session_timeout_ms * 1000 / 3, MAX_SLEEP_TIME * 1000 * 1000));

			LOG_WARNING(log, "Error on attempt " << i << ": " << error2string(code)  << ". Retry");
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
	int32_t session_timeout_ms;

	Poco::FastMutex mutex;
	ACLPtr default_acl;
	zhandle_t * impl;

	std::unordered_set<WatchWithEvent *> watch_store;

	/// Количество попыток повторить операцию чтения при OperationTimeout, ConnectionLoss
	size_t retry_num = 3;
	Logger * log = nullptr;
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
		catch (const KeeperException & e)
		{
			LOG_ERROR(zookeeper.log, "~EphemeralNodeHolder(): " << e.displayText());
		}
	}

private:
	std::string path;
	ZooKeeper & zookeeper;
};

typedef EphemeralNodeHolder::Ptr EphemeralNodeHolderPtr;

}
