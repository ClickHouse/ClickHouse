#pragma once

#include <DB/Dictionaries/IDictionarySource.h>
#include <DB/Dictionaries/MongoDBBlockInputStream.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <mongo/client/dbclient.h>


namespace DB
{

/// Allows loading dictionaries from a MongoDB collection
class MongoDBDictionarySource final : public IDictionarySource
{
	MongoDBDictionarySource(
		const DictionaryStructure & dict_struct, const std::string & host, const std::string & port,
		const std::string & user, const std::string & password,
		const std::string & db, const std::string & collection,
		const Block & sample_block, Context & context)
		: dict_struct{dict_struct}, host{host}, port{port}, user{user}, password{password},
		  db{db}, collection{collection}, sample_block{sample_block}, context(context),
		  connection{true}
	{
		init();

		connection.connect(host + ':' + port);

		/// @todo: should connection.auth be called after or before .connect ?
		if (!user.empty())
		{
			std::string error;
			if (!connection.auth(db, user, password, error))
				throw DB::Exception{
					"Could not authenticate to a MongoDB database " + db + " with provided credentials: " + error,
					ErrorCodes::WRONG_PASSWORD
				};
		}

		/// compose BSONObj containing all requested fields
		mongo::BSONObjBuilder builder;
		builder << "_id" << 0;

		for (const auto & column : sample_block.getColumns())
			builder << column.name << 1;

		fields_to_query = builder.obj();
	}

	/// mongo-cxx driver requires global initialization before using any functionality
	static void init()
	{
		static const auto mongo_init_status = mongo::client::initialize();

		if (!mongo_init_status.isOK())
			throw DB::Exception{
					"mongo::client::initialize() failed: " + mongo_init_status.toString(),
					ErrorCodes::MONGODB_INIT_FAILED
			};

		LOG_TRACE(&Logger::get("MongoDBDictionarySource"), "mongo::client::initialize() ok");
	}

public:
	MongoDBDictionarySource(
		const DictionaryStructure & dict_struct, const Poco::Util::AbstractConfiguration & config,
		const std::string & config_prefix, Block & sample_block, Context & context)
		: MongoDBDictionarySource{
			dict_struct,
			config.getString(config_prefix + ".host"),
			config.getString(config_prefix + ".port"),
			config.getString(config_prefix + ".user", ""),
			config.getString(config_prefix + ".password", ""),
			config.getString(config_prefix + ".db", ""),
			config.getString(config_prefix + ".collection"),
			sample_block, context
		}
	{
	}

	MongoDBDictionarySource(const MongoDBDictionarySource & other)
		: MongoDBDictionarySource{
			other.dict_struct, other.host, other.port, other.user, other.password,
			other.db, other.collection, other.sample_block, other.context
		}
	{
	}

	BlockInputStreamPtr loadAll() override
	{
		return new MongoDBBlockInputStream{
			connection.query(db + '.' + collection, {}, 0, 0, &fields_to_query),
			sample_block, 8192
		};
	}

	bool supportsSelectiveLoad() const override { return true; }

	BlockInputStreamPtr loadIds(const std::vector<std::uint64_t> & ids) override
	{
		/// @todo: convert ids to a BSONObj with $in and enumeration, pass as second argument to .query
		return new MongoDBBlockInputStream{
			connection.query(db + '.' + collection, {}, 0, 0, &fields_to_query),
			sample_block, 8192
		};
	}

	bool isModified() const override { return false; }

	DictionarySourcePtr clone() const override { return std::make_unique<MongoDBDictionarySource>(*this); }

	std::string toString() const override
	{
		return "MongoDB: " + db + '.' + collection + ',' + (user.empty() ? " " : " " + user + '@') + host + ':' + port;
	}

private:
	const DictionaryStructure dict_struct;
	const std::string host;
	const std::string port;
	const std::string user;
	const std::string password;
	const std::string db;
	const std::string collection;
	Block sample_block;
	Context & context;

	mongo::DBClientConnection connection;
	mongo::BSONObj fields_to_query;
};

}
