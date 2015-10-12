#pragma once
#include <common/Common.h>
#include <boost/function.hpp>
#include <future>
#include <boost/noncopyable.hpp>
#include <boost/ptr_container/ptr_vector.hpp>
#include <zookeeper/zookeeper.h>
#include <Poco/SharedPtr.h>
#include <Poco/Event.h>

namespace zkutil
{
typedef const ACL_vector * ACLPtr;
typedef Stat Stat;

struct Op
{
public:
	Op() : data(new zoo_op_t) {}
	virtual ~Op() {}

	std::unique_ptr<zoo_op_t> data;

	struct Remove;
	struct Create;
	struct SetData;
	struct Check;
};

struct Op::Remove : public Op
{
	Remove(const std::string & path_, int32_t version) :
		path(path_)
	{
		zoo_delete_op_init(data.get(), path.c_str(), version);
	}

private:
	std::string path;
};

struct Op::Create : public Op
{
	Create(const std::string & path_, const std::string & value_, ACLPtr acl, int32_t flags);

	std::string getPathCreated()
	{
		return created_path.data();
	}

private:
	std::string path;
	std::string value;
	std::vector<char> created_path;
};

struct Op::SetData : public Op
{
	SetData(const std::string & path_, const std::string & value_, int32_t version) :
		path(path_), value(value_)
	{
		zoo_set_op_init(data.get(), path.c_str(), value.c_str(), value.size(), version, &stat);
	}
private:
	std::string path;
	std::string value;
	Stat stat;
};

struct Op::Check : public Op
{
	Check(const std::string & path_, int32_t version) :
		path(path_)
	{
		zoo_check_op_init(data.get(), path.c_str(), version);
	}
private:
	std::string path;
};

struct OpResult : public zoo_op_result_t
{
	/// Указатели в этой структуре указывают на поля в классе Op.
	/// Поэтому деструктор не нужен
};

typedef boost::ptr_vector<Op> Ops;
typedef std::vector<OpResult> OpResults;
typedef std::shared_ptr<OpResults> OpResultsPtr;
typedef std::vector<std::string> Strings;

namespace CreateMode
{
	extern const int Persistent;
	extern const int Ephemeral;
	extern const int EphemeralSequential;
	extern const int PersistentSequential;
}

typedef Poco::SharedPtr<Poco::Event> EventPtr;

}
