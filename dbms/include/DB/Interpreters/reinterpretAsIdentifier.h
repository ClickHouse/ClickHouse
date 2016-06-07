#pragma once

#include <Poco/SharedPtr.h>


namespace DB
{

class IAST;
class ASTIdentifier;
class Context;

/** \brief if `expr` is not already ASTIdentifier evaluates it
  *  and replaces by a new ASTIdentifier with the result of evaluation as its name.
  *  `expr` must evaluate to a String type */
ASTIdentifier & reinterpretAsIdentifier(Poco::SharedPtr<IAST> & expr, const Context & context);

}
