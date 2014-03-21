#pragma once
#include <zkutil/Types.h>
#include <zkutil/KeeperException.h>
#include <Poco/Util/LayeredConfiguration.h>


namespace zkutil
{

const UInt32 DEFAULT_SESSION_TIMEOUT = 30000;

/** Сессия в ZooKeeper. Интерфейс существенно отличается от обычного API ZooKeeper.
  * Вместо callback-ов для watch-ей используются std::future.
  * Методы с названиями, не начинающимися с try, бросают исключение при любой ошибке.
  * Методы с названиями, начинающимися с try, не бросают исключение только при перечисленных видах ошибок.
  * Например, исключение бросается в любом случае, если сессия разорвалась или если не хватает прав или ресурсов.
  */
class ZooKeeper
{
public:
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
	ZooKeeper(const Poco::Util::LayeredConfiguration & config, const std::string & config_name,
			  WatchFunction * watch = nullptr);

	/** Возвращает true, если сессия навсегда завершена.
	  * Это возможно только если соединение было установлено, а потом разорвалось. Это достаточно редкая ситуация.
	  * С другой стороны, если, например, указан неправильный сервер или порт, попытки соединения будут продолжаться бесконечно,
	  * disconnected() будет возвращать false, и все вызовы будут выбрасывать исключение ConnectionLoss.
	  */
	bool disconnected();

	void setDefaultACL(ACLs & acl);

	ACLs getDefaultACL();

	/** Создать znode. Используется ACL, выставленный вызовом setDefaultACL (по умолчанию, всем полный доступ).
	  * Если что-то пошло не так, бросить исключение.
	  */
	std::string create(const std::string & path, const std::string & data, CreateMode::type mode);

	/** Не бросает исключение при следующих ошибках:
	  *  - Нет родителя создаваемой ноды.
	  *  - Родитель эфемерный.
	  *  - Такая нода уже есть.
	  * При остальных ошибках бросает исключение.
	  */
	ReturnCode::type tryCreate(const std::string & path, const std::string & data, CreateMode::type mode, std::string & pathCreated);

	/** Удалить ноду, если ее версия равна version (если -1, подойдет любая версия).
	  */
	void remove(const std::string & path, int32_t version = -1);

	/** Не бросает исключение при следующих ошибках:
	  *  - Такой ноды нет.
	  *  - У ноды другая версия.
	  *  - У ноды есть дети.
	  */
	ReturnCode::type tryRemove(const std::string & path, int32_t version = -1);

	bool exists(const std::string & path, Stat * stat = nullptr, WatchFuture * watch = nullptr);

	std::string get(const std::string & path, Stat * stat, WatchFuture * watch);

	/** Не бросает исключение при следующих ошибках:
	  *  - Такой ноды нет. В таком случае возвращает false.
	  */
	bool tryGet(const std::string & path, std::string & res, Stat * stat = nullptr, WatchFuture * watch = nullptr);

	void set(const std::string & path, const std::string & data,
			int32_t version = -1, Stat * stat = nullptr);

	/** Не бросает исключение при следующих ошибках:
	  *  - Такой ноды нет.
	  *  - У ноды другая версия.
	  */
	ReturnCode::type trySet(const std::string & path, const std::string & data,
							int32_t version = -1, Stat * stat = nullptr);

	Strings getChildren(const std::string & path,
						Stat * stat = nullptr,
						WatchFuture * watch = nullptr);

	/** Не бросает исключение при следующих ошибках:
	  *  - Такой ноды нет. В таком случае возвращает false.
	  */
	bool tryGetChildren(const std::string & path, Strings & res,
						Stat * stat = nullptr,
						WatchFuture * watch = nullptr);

	void close();

	/** Транзакционно выполняет несколько операций. При любой ошибке бросает исключение.
	  */
	OpResultsPtr multi(const Ops & ops);

	/** Бросает исключение только если какая-нибудь операция вернула "неожиданную" ошибку - такую ошибку,
	  * увидев которую соответствующий метод try* бросил бы исключение. */
	OpResultsPtr tryMulti(const Ops & ops);

private:
	void init(const std::string & hosts, int32_t sessionTimeoutMs, WatchFunction * watch_);
	friend struct StateWatch;

	zk::ZooKeeper impl;
	WatchFunction * state_watch;

	Poco::FastMutex mutex;
	ACLs default_acl;
	SessionState::type session_state;

	void stateChanged(WatchEvent::type event, SessionState::type state, const std::string& path);
};

}
