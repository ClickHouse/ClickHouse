#pragma once

#include <Client/ClientBase.h>
#include <Client/Suggest.h>
#include <Poco/Util/Application.h>

namespace DB
{


class ClientApplicationBase : public ClientBase, public Poco::Util::Application, public IHints<2>
{

}

}
