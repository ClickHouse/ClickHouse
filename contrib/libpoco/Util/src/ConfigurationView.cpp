//
// ConfigurationView.cpp
//
// $Id: //poco/1.4/Util/src/ConfigurationView.cpp#1 $
//
// Library: Util
// Package: Configuration
// Module:  ConfigurationView
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Util/ConfigurationView.h"


namespace Poco {
namespace Util {


ConfigurationView::ConfigurationView(const std::string& prefix, AbstractConfiguration* pConfig):
	_prefix(prefix),
	_pConfig(pConfig)
{
	poco_check_ptr (pConfig);

	_pConfig->duplicate();
}


ConfigurationView::~ConfigurationView()
{
	_pConfig->release();
}


bool ConfigurationView::getRaw(const std::string& key, std::string& value) const
{
	std::string translatedKey = translateKey(key);
	return _pConfig->getRaw(translatedKey, value) || _pConfig->getRaw(key, value);
}


void ConfigurationView::setRaw(const std::string& key, const std::string& value)
{
	std::string translatedKey = translateKey(key);
	_pConfig->setRaw(translatedKey, value); 
}


void ConfigurationView::enumerate(const std::string& key, Keys& range) const
{
	std::string translatedKey = translateKey(key);
	_pConfig->enumerate(translatedKey, range);
}


void ConfigurationView::removeRaw(const std::string& key)
{
	std::string translatedKey = translateKey(key);
	_pConfig->remove(translatedKey);
}


std::string ConfigurationView::translateKey(const std::string& key) const
{
	std::string result = _prefix;
	if (!result.empty() && !key.empty() && key[0] != '[') result += '.';
	result += key;
	return result;
}


} } // namespace Poco::Util
