//
// UUIDGenerator.cpp
//
// $Id: //poco/1.4/Foundation/src/UUIDGenerator.cpp#1 $
//
// Library: Foundation
// Package: UUID
// Module:  UUID
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/UUIDGenerator.h"
#include "Poco/Thread.h"
#include "Poco/RandomStream.h"
#include "Poco/DigestEngine.h"
#include "Poco/MD5Engine.h"
#include "Poco/SHA1Engine.h"
#include "Poco/SingletonHolder.h"
#include <cstring>


namespace Poco {


UUIDGenerator::UUIDGenerator(): _ticks(0), _haveNode(false)
{
}


UUIDGenerator::~UUIDGenerator()
{
}


UUID UUIDGenerator::create()
{
	FastMutex::ScopedLock lock(_mutex);

	if (!_haveNode)
	{
		Environment::nodeId(_node);
		_haveNode = true;
	}
	Timestamp::UtcTimeVal tv = timeStamp();
	UInt32 timeLow = UInt32(tv & 0xFFFFFFFF);
	UInt16 timeMid = UInt16((tv >> 32) & 0xFFFF);
	UInt16 timeHiAndVersion = UInt16((tv >> 48) & 0x0FFF) + (UUID::UUID_TIME_BASED << 12);
	UInt16 clockSeq = (UInt16(_random.next() >> 4) & 0x3FFF) | 0x8000;
	return UUID(timeLow, timeMid, timeHiAndVersion, clockSeq, _node);
}


UUID UUIDGenerator::createFromName(const UUID& nsid, const std::string& name)
{
	MD5Engine md5;
	return createFromName(nsid, name, md5);
}


UUID UUIDGenerator::createFromName(const UUID& nsid, const std::string& name, DigestEngine& de)
{
	UUID::Version version = UUID::UUID_NAME_BASED;
	if (dynamic_cast<SHA1Engine*>(&de)) version = UUID::UUID_NAME_BASED_SHA1;
	return createFromName(nsid, name, de, version);
}


UUID UUIDGenerator::createFromName(const UUID& nsid, const std::string& name, DigestEngine& de, UUID::Version version)
{
	poco_assert_dbg (de.digestLength() >= 16);

	UUID netNsid = nsid;
	netNsid.toNetwork();
	de.reset();
	de.update(&netNsid._timeLow, sizeof(netNsid._timeLow));
	de.update(&netNsid._timeMid, sizeof(netNsid._timeMid));
	de.update(&netNsid._timeHiAndVersion, sizeof(netNsid._timeHiAndVersion));
	de.update(&netNsid._clockSeq, sizeof(netNsid._clockSeq));
	de.update(&netNsid._node[0], sizeof(netNsid._node));
	de.update(name);
	char buffer[16];
	const DigestEngine::Digest& d = de.digest();
	for (int i = 0; i < 16; ++i)
	{
		buffer[i] = d[i];
	}
	return UUID(buffer, version);
}

	
UUID UUIDGenerator::createRandom()
{
	char buffer[16];
	RandomInputStream ris;
	ris.read(buffer, sizeof(buffer));
	return UUID(buffer, UUID::UUID_RANDOM);
}


Timestamp::UtcTimeVal UUIDGenerator::timeStamp()
{
	Timestamp now;
	for (;;)
	{
		if (now != _lastTime)
		{
			_lastTime = now;
			_ticks = 0;
			break;
		}
		if (_ticks < 100)
		{
			++_ticks;
			break;
		}
		now.update();
	}
	Timestamp::UtcTimeVal tv = now.utcTime();
	return tv + _ticks;
}


UUID UUIDGenerator::createOne()
{
	try
	{
		return create();
	}
	catch (Exception&)
	{
		return createRandom();
	}
}


namespace
{
	static SingletonHolder<UUIDGenerator> sh;
}


UUIDGenerator& UUIDGenerator::defaultGenerator()
{
	return *sh.get();
}


} // namespace Poco
