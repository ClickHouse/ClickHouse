#pragma once

#include <functional>
#include <memory>

#include <DB/Core/Types.h>
#include <DB/Core/NamesAndTypes.h>
#include <DB/Interpreters/Settings.h>
#include <DB/Storages/IStorage.h>
#include <DB/IO/CompressedStream.h>

#include <Poco/Net/IPAddress.h>


namespace zkutil
{
	class ZooKeeper;
}


namespace DB
{

class ContextShared;
class QuotaForIntervals;
class TableFunctionFactory;
class AggregateFunctionFactory;
class FormatFactory;
class Dictionaries;
class ExternalDictionaries;
class InterserverIOHandler;
class BackgroundProcessingPool;
class MergeList;
class Cluster;
class Compiler;
class MarkCache;
class UncompressedCache;
class ProcessList;
class ProcessListElement;
class Macros;
class Progress;
class Clusters;
class QueryLog;
struct MergeTreeSettings;


/// (имя базы данных, имя таблицы)
typedef std::pair<String, String> DatabaseAndTableName;

/// Таблица -> множество таблиц-представлений, которые деляют SELECT из неё.
typedef std::map<DatabaseAndTableName, std::set<DatabaseAndTableName>> ViewDependencies;
typedef std::vector<DatabaseAndTableName> Dependencies;


/** Набор известных объектов, которые могут быть использованы в запросе.
  * Состоит из разделяемой части (всегда общей для всех сессий и запросов)
  *  и копируемой части (которая может быть своей для каждой сессии или запроса).
  *
  * Всё инкапсулировано для всяких проверок и блокировок.
  */
class Context
{
public:
	enum class Interface
	{
		TCP = 1,
		HTTP = 2,
		OLAP_HTTP = 3,
	};

	enum class HTTPMethod
	{
		UNKNOWN = 0,
		GET = 1,
		POST = 2,
	};

private:
	typedef std::shared_ptr<ContextShared> Shared;
	Shared shared;

	String user;						/// Текущий пользователь.
	Poco::Net::IPAddress ip_address;	/// IP-адрес, с которого задан запрос.
	Interface interface = Interface::TCP;
	HTTPMethod http_method = HTTPMethod::UNKNOWN;	/// NOTE Возможно, перенести это в отдельный struct ClientInfo.

	std::shared_ptr<QuotaForIntervals> quota;	/// Текущая квота. По-умолчанию - пустая квота, которая ничего не ограничивает.
	String current_database;			/// Текущая БД.
	String current_query_id;			/// Id текущего запроса.
	Settings settings;					/// Настройки выполнения запроса.
	using ProgressCallback = std::function<void(const Progress & progress)>;
	ProgressCallback progress_callback;	/// Колбек для отслеживания прогресса выполнения запроса.
	ProcessListElement * process_list_elem = nullptr;	/// Для отслеживания общего количества потраченных на запрос ресурсов.

	String default_format;	/// Формат, используемый, если сервер сам форматирует данные, и если в запросе не задан FORMAT.
							/// То есть, используется в HTTP-интерфейсе. Может быть не задан - тогда используется некоторый глобальный формат по-умолчанию.
	Tables external_tables;				/// Временные таблицы.
	Context * session_context = nullptr;	/// Контекст сессии или nullptr, если его нет. (Возможно, равен this.)
	Context * global_context = nullptr;		/// Глобальный контекст или nullptr, если его нет. (Возможно, равен this.)

public:
	Context();
	~Context();

	String getPath() const;
	String getTemporaryPath() const;
	void setPath(const String & path);
	void setTemporaryPath(const String & path);

	using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

	/** Забрать список пользователей, квот и профилей настроек из этого конфига.
	  * Список пользователей полностью заменяется.
	  * Накопленные значения у квоты не сбрасываются, если квота не удалена.
	  */
	void setUsersConfig(ConfigurationPtr config);

	ConfigurationPtr getUsersConfig();

	void setUser(const String & name, const String & password, const Poco::Net::IPAddress & address, const String & quota_key);
	String getUser() const { return user; }
	Poco::Net::IPAddress getIPAddress() const { return ip_address; }

	Interface getInterface() const { return interface; }
	void setInterface(Interface interface_) { interface = interface_; }

	HTTPMethod getHTTPMethod() const { return http_method; }
	void setHTTPMethod(HTTPMethod http_method_) { http_method = http_method_; }

	void setQuota(const String & name, const String & quota_key, const String & user_name, const Poco::Net::IPAddress & address);
	QuotaForIntervals & getQuota();

	void addDependency(const DatabaseAndTableName & from, const DatabaseAndTableName & where);
	void removeDependency(const DatabaseAndTableName & from, const DatabaseAndTableName & where);
	Dependencies getDependencies(const String & database_name, const String & table_name) const;

	/// Проверка существования таблицы/БД. database может быть пустой - в этом случае используется текущая БД.
	bool isTableExist(const String & database_name, const String & table_name) const;
	bool isDatabaseExist(const String & database_name) const;
	void assertTableExists(const String & database_name, const String & table_name) const;

	/** Параметр check_database_access_rights существует, чтобы не проверить повторно права доступа к БД,
	  * когда assertTableDoesnExist или assertDatabaseExists вызывается внутри другой функции, которая уже
	  * сделала эту проверку.
	  */
	void assertTableDoesntExist(const String & database_name, const String & table_name, bool check_database_acccess_rights = true) const;
	void assertDatabaseExists(const String & database_name, bool check_database_acccess_rights = true) const;

	void assertDatabaseDoesntExist(const String & database_name) const;

	Tables getExternalTables() const;
	StoragePtr tryGetExternalTable(const String & table_name) const;
	StoragePtr getTable(const String & database_name, const String & table_name) const;
	StoragePtr tryGetTable(const String & database_name, const String & table_name) const;
	void addExternalTable(const String & table_name, StoragePtr storage);
	void addTable(const String & database_name, const String & table_name, StoragePtr table);
	void addDatabase(const String & database_name);

	/// Возвращает отцепленную таблицу.
	StoragePtr detachTable(const String & database_name, const String & table_name);

	void detachDatabase(const String & database_name);

	String getCurrentDatabase() const;
	String getCurrentQueryId() const;
	void setCurrentDatabase(const String & name);
	void setCurrentQueryId(const String & query_id);

	String getDefaultFormat() const;	/// Если default_format не задан - возвращается некоторый глобальный формат по-умолчанию.
	void setDefaultFormat(const String & name);

	const Macros & getMacros() const;
	void setMacros(Macros && macros);

	Settings getSettings() const;
	void setSettings(const Settings & settings_);

	Limits getLimits() const;

	/// Установить настройку по имени.
	void setSetting(const String & name, const Field & value);

	/// Установить настройку по имени. Прочитать значение в текстовом виде из строки (например, из конфига, или из параметра URL).
	void setSetting(const String & name, const std::string & value);

	const TableFunctionFactory & getTableFunctionFactory() const;
	const AggregateFunctionFactory & getAggregateFunctionFactory() const;
	const FormatFactory & getFormatFactory() const;
	const Dictionaries & getDictionaries() const;
	const ExternalDictionaries & getExternalDictionaries() const;
	void tryCreateDictionaries() const;
	void tryCreateExternalDictionaries() const;

	InterserverIOHandler & getInterserverIOHandler();

	/// Как другие серверы могут обратиться к этому для скачивания реплицируемых данных.
	void setInterserverIOAddress(const String & host, UInt16 port);
	std::pair<String, UInt16> getInterserverIOAddress() const;
	/// Порт, который сервер слушает для выполнения SQL-запросов.
	UInt16 getTCPPort() const;

	/// Получить запрос на CREATE таблицы.
	ASTPtr getCreateQuery(const String & database_name, const String & table_name) const;

	/// Для методов ниже может быть необходимо захватывать mutex самостоятельно.
	Poco::Mutex & getMutex() const;

	/// Метод getDatabases не потокобезопасен. При работе со списком БД и таблиц, вы должны захватить mutex.
	const Databases & getDatabases() const;
	Databases & getDatabases();

	Context & getSessionContext();
	Context & getGlobalContext();

	void setSessionContext(Context & context_)								{ session_context = &context_; }
	void setGlobalContext(Context & context_)								{ global_context = &context_; }

	const Settings & getSettingsRef() const { return settings; };
	Settings & getSettingsRef() { return settings; };

	void setProgressCallback(ProgressCallback callback);
	/// Используется в InterpreterSelectQuery, чтобы передать его в IProfilingBlockInputStream.
	ProgressCallback getProgressCallback() const;

	/** Устанавливается в executeQuery и InterpreterSelectQuery. Затем используется в IProfilingBlockInputStream,
	  *  чтобы обновлять и контролировать информацию об общем количестве потраченных на запрос ресурсов.
	  */
	void setProcessListElement(ProcessListElement * elem);
	/// Может вернуть nullptr, если запрос не был вставлен в ProcessList.
	ProcessListElement * getProcessListElement();

	/// Список всех запросов.
	ProcessList & getProcessList();
	const ProcessList & getProcessList() const;

	MergeList & getMergeList();
	const MergeList & getMergeList() const;

	/// Создать кэш разжатых блоков указанного размера. Это можно сделать только один раз.
	void setUncompressedCache(size_t max_size_in_bytes);
	std::shared_ptr<UncompressedCache> getUncompressedCache() const;

	void setZooKeeper(std::shared_ptr<zkutil::ZooKeeper> zookeeper);
	/// Если в момент вызова текущая сессия просрочена, синхронно создает и возвращает новую вызовом startNewSession().
	std::shared_ptr<zkutil::ZooKeeper> getZooKeeper() const;

	/// Создать кэш засечек указанного размера. Это можно сделать только один раз.
	void setMarkCache(size_t cache_size_in_bytes);
	std::shared_ptr<MarkCache> getMarkCache() const;

	BackgroundProcessingPool & getBackgroundPool();

	/** Очистить кэши разжатых блоков и засечек.
	  * Обычно это делается при переименовании таблиц, изменении типа столбцов, удалении таблицы.
	  *  - так как кэши привязаны к именам файлов, и становятся некорректными.
	  *  (при удалении таблицы - нужно, так как на её месте может появиться другая)
	  * const - потому что изменение кэша не считается существенным.
	  */
	void resetCaches() const;

	void initClusters();
	Cluster & getCluster(const std::string & cluster_name);
	Poco::SharedPtr<Clusters> getClusters() const;

	Compiler & getCompiler();
	QueryLog & getQueryLog();
	const MergeTreeSettings & getMergeTreeSettings();

	/// Позволяет выбрать метод сжатия по условиям, описанным в конфигурационном файле.
	CompressionMethod chooseCompressionMethod(size_t part_size, double part_size_ratio) const;

	void shutdown();

private:
	/** Проверить, имеет ли текущий клиент доступ к заданной базе данных.
	  * Если доступ запрещён, кинуть исключение.
	  * NOTE: Этот метод надо всегда вызывать при захваченном мьютексе shared->mutex.
	  */
	void checkDatabaseAccessRights(const std::string & database_name) const;

	const Dictionaries & getDictionariesImpl(bool throw_on_error) const;
	const ExternalDictionaries & getExternalDictionariesImpl(bool throw_on_error) const;

	StoragePtr getTableImpl(const String & database_name, const String & table_name, Exception * exception) const;
};


}
