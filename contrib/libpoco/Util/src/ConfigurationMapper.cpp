//
// ConfigurationMapper.cpp
//
// $Id: //poco/1.4/Util/src/ConfigurationMapper.cpp#1 $
//
// Library: Util
// Package: Configuration
// Module:  ConfigurationMapper
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Util/ConfigurationMapper.h"


namespace Poco {
namespace Util {


ConfigurationMapper::ConfigurationMapper(const std::string& fromPrefix, const std::string& toPrefix, AbstractConfiguration* pConfig):
	_fromPrefix(fromPrefix),
	_toPrefix(toPrefix),
	_pConfig(pConfig)
{
	poco_check_ptr (pConfig);

	if (!_fromPrefix.empty()) _fromPrefix += '.';
	if (!_toPrefix.empty()) _toPrefix += '.';

	_pConfig->duplicate();
}


ConfigurationMapper::~ConfigurationMapper()
{
	_pConfig->release();
}


bool ConfigurationMapper::getRaw(const std::string& key, std::string& value) const
{
	std::string translatedKey = translateKey(key);
	return _pConfig->getRaw(translatedKey, value);
}


void ConfigurationMapper::setRaw(const std::string& key, const std::string& value)
{
	std::string translatedKey = translateKey(key);
	_pConfig->setRaw(translatedKey, value); 
}


void ConfigurationMapper::enumerate(const std::string& key, Keys& range) const
{
	std::string cKey(key);
	if (!cKey.empty()) cKey += '.';
	std::string::size_type keyLen = cKey.length();
	if (keyLen < _toPrefix.length())
	{
		if (_toPrefix.compare(0, keyLen, cKey) == 0)
		{
			std::string::size_type pos = _toPrefix.find_first_of('.', keyLen);
			poco_assert_dbg(pos != std::string::npos);
			range.push_back(_toPrefix.substr(keyLen, pos - keyLen));
		}
	}
	else
	{
		std::string translatedKey;
		if (cKey == _toPrefix)
		{
			translatedKey = _fromPrefix;
			if (!translatedKey.empty())
				translatedKey.resize(translatedKey.length() - 1);
		}
		else translatedKey = translateKey(key);
		_pConfig->enumerate(translatedKey, range);
	}
}


void ConfigurationMapper::removeRaw(const std::string& key)
{
	std::string translatedKey = translateKey(key);
	_pConfig->remove(translatedKey);
}


std::string ConfigurationMapper::translateKey(const std::string& key) const
{
	std::string result(key);
	if (result.compare(0, _toPrefix.size(), _toPrefix) == 0)
		result.replace(0, _toPrefix.size(), _fromPrefix);
	return result;
}


} } // namespace Poco::Util
