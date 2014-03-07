#pragma once
#include <zookeeper/zookeeper.hh>
#include <Yandex/Common.h>
#include <boost/function.hpp>
#include <future>


namespace zkutil
{

const UInt32 DEFAULT_SESSION_TIMEOUT = 30000;

namespace zk = org::apache::zookeeper;
namespace CreateMode = zk::CreateMode;
namespace ReturnCode = zk::ReturnCode;
namespace SessionState = zk::SessionState;
namespace WatchEvent = zk::WatchEvent;

typedef zk::data::Stat Stat;
typedef zk::data::ACL ACL;

typedef std::vector<ACL> ACLs;
typedef std::vector<std::string> Strings;

typedef boost::function<void (WatchEvent::type event, SessionState::type state,
                         const std::string & path)> WatchFunction;

struct WatchEventInfo
{
	WatchEvent::type event;
	SessionState::type state;
	std::string path;

	WatchEventInfo() {}
	WatchEventInfo(WatchEvent::type event_, SessionState::type state_, const std::string & path_)
		: event(event_), state(state_), path(path_) {}
};

typedef std::future<WatchEventInfo> WatchFuture;

/** Сессия в ZooKeeper. Интерфейс существенно отличается от обычного API ZooKeeper.
  * Вместо callback-ов для watch-ей используются std::future.
  * Методы с названиями, не начинающимися с try, бросают исключение при любой ошибке.
  * Методы с названиями, начинающимися с try, не бросают исключение только при некторых видах ошибок.
  * Например, исключение бросается в любом случае, если сессия разорвалась или если не хватает прав или ресурсов.
  */
class ZooKeeper
{
public:
	ZooKeeper(const std::string & hosts, int32_t sessionTimeoutMs = DEFAULT_SESSION_TIMEOUT, WatchFunction * watch = nullptr);

	/** Возвращает true, если сессия навсегда завершена.
	  * Это возможно только если соединение было установлено, а потом разорвалось. Это достаточно редкая ситуация.
	  * С другой стороны, если, например, указан неправильный сервер или порт, попытки соединения будут продолжаться бесконечно,
	  * disconnected() будет возвращать false, и все вызовы будут выбрасывать исключение ConnectionLoss.
	  */
	bool disconnected();

	void setDefaultACL(ACLs & acl);

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

	/// Возвращает false, если нет такой ноды. При остальных ошибках бросает исключение.
	bool tryGet(const std::string & path, std::string & data, Stat * stat = nullptr, WatchFuture * watch = nullptr);

	void set(const std::string & path, const std::string & data,
			int32_t version = -1, Stat * stat = nullptr);

	Strings getChildren(const std::string & path,
						Stat * stat = nullptr,
						WatchFuture * watch = nullptr);

	void close();

	boost::ptr_vector<zk::OpResult> & multi(const boost::ptr_vector<zk::Op> & ops);

private:
	friend struct StateWatch;

	zk::ZooKeeper impl;
	ACLs default_acl;
	WatchFunction * state_watch;
	SessionState::type session_state;

	void stateChanged(WatchEvent::type event, SessionState::type state, const std::string& path);
};

}
