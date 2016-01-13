//
// TwitterApp.cpp
//
// $Id: //poco/1.4/Net/samples/TwitterClient/src/TweetApp.cpp#2 $
//
// A very simple command-line Twitter client.
//
// Copyright (c) 2009-2013, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Util/Application.h"
#include "Poco/Util/Option.h"
#include "Poco/Util/OptionSet.h"
#include "Poco/Util/HelpFormatter.h"
#include "Twitter.h"
#include <iostream>


using Poco::Util::Application;
using Poco::Util::Option;
using Poco::Util::OptionSet;
using Poco::Util::HelpFormatter;
using Poco::Util::AbstractConfiguration;
using Poco::Util::OptionCallback;


class TweetApp: public Application
	/// A very simple Twitter command-line client.
{
public:
	TweetApp()
	{
	}
	
protected:	
	void defineOptions(OptionSet& options)
	{
		Application::defineOptions(options);

		options.addOption(
			Option("help", "h", "Display help information on command line arguments.")
				.required(false)
				.repeatable(false)
				.callback(OptionCallback<TweetApp>(this, &TweetApp::handleHelp)));

		options.addOption(
			Option("message", "m", "Specify the status message to post.")
				.required(true)
				.repeatable(false)
				.argument("message")
				.callback(OptionCallback<TweetApp>(this, &TweetApp::handleMessage)));
				
		options.addOption(
			Option("ckey", "c", "Specify the Twitter consumer key.")
				.required(true)
				.repeatable(false)
				.argument("consumer key")
				.callback(OptionCallback<TweetApp>(this, &TweetApp::handleConsumerKey)));
				
		options.addOption(
			Option("csecret", "s", "Specify the Twitter consumer secret.")
				.required(true)
				.repeatable(false)
				.argument("consumer secret")
				.callback(OptionCallback<TweetApp>(this, &TweetApp::handleConsumerSecret)));

		options.addOption(
			Option("token", "t", "Specify the Twitter access token.")
				.required(true)
				.repeatable(true)
				.argument("access token")
				.callback(OptionCallback<TweetApp>(this, &TweetApp::handleAccessToken)));

		options.addOption(
			Option("tsecret", "S", "Specify the Twitter access token secret.")
				.required(true)
				.repeatable(true)
				.argument("access token secret")
				.callback(OptionCallback<TweetApp>(this, &TweetApp::handleAccessTokenSecret)));
	}
	
	void handleHelp(const std::string& name, const std::string& value)
	{
		displayHelp();
		stopOptionsProcessing();
	}
	
	void handleConsumerKey(const std::string& name, const std::string& value)
	{
		_consumerKey = value;
	}
	
	void handleConsumerSecret(const std::string& name, const std::string& value)
	{
		_consumerSecret = value;
	}

	void handleAccessToken(const std::string& name, const std::string& value)
	{
		_accessToken = value;
	}

	void handleAccessTokenSecret(const std::string& name, const std::string& value)
	{
		_accessTokenSecret = value;
	}
	
	void handleMessage(const std::string& name, const std::string& value)
	{
		_message = value;
	}
		
	void displayHelp()
	{
		HelpFormatter helpFormatter(options());
		helpFormatter.setCommand(commandName());
		helpFormatter.setUsage("OPTIONS");
		helpFormatter.setHeader("A simple Twitter command line client for posting status updates.");
		helpFormatter.format(std::cout);
	}
	
	int main(const std::vector<std::string>& args)
	{
		try
		{
			if (!_message.empty())
			{
				Twitter twitter;
				twitter.login(_consumerKey, _consumerSecret, _accessToken, _accessTokenSecret);
				Poco::Int64 statusId = twitter.update(_message);
				std::cout << statusId << std::endl;
			}
		}
		catch (Poco::Exception& exc)
		{
			std::cerr << exc.displayText() << std::endl;
			return Application::EXIT_SOFTWARE;
		}
		return Application::EXIT_OK;
	}

private:
	std::string _consumerKey;
	std::string _consumerSecret;
	std::string _accessToken;
	std::string _accessTokenSecret;
	std::string _message;
};


POCO_APP_MAIN(TweetApp)
