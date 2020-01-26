/**
 *  Flags.cpp
 *
 *  The various flags that are supported
 *
 *  @copyright 2014 - 2018 Copernica BV
 */
#include "includes.h"
 
/**
 *  Set up namespace
 */
namespace AMQP {

/**
 *  All bit flags
 *  @var int
 */
const int durable      = 0x1;
const int autodelete   = 0x2;
const int active       = 0x4;
const int passive      = 0x8;
const int ifunused     = 0x10;
const int ifempty      = 0x20;
const int global       = 0x40;
const int nolocal      = 0x80;
const int noack        = 0x100;
const int exclusive    = 0x200;
const int nowait       = 0x400;
const int mandatory    = 0x800;
const int immediate    = 0x1000;
const int redelivered  = 0x2000;
const int multiple     = 0x4000;
const int requeue      = 0x8000;
const int internal     = 0x10000;

/**
 *  Flags for event loops
 */
const int readable      = 0x1;
const int writable      = 0x2;

/**
 *  End of namespace
 */
}

