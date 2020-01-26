/**
 *  OpenSSL.h
 * 
 *  Function to set openssl features
 * 
 *  @author Emiel Bruijntjes <emiel.bruijntjes@copernica.com>
 *  @copyright 2018 Copernica BV
 */

/**
 *  Include guard
 */
#pragma once

/**
 *  Begin of namespace
 */
namespace AMQP {

/**
 *  To make secure "amqps://" connections, AMQP-CPP relies on functions from the 
 *  openssl library. It your application is dynamically linked to openssl (because
 *  it was compiled with the "-lssl" flag), this works flawlessly because AMQPCPP 
 *  can then locate the openssl symbols in its own project space. However, if the
 *  openssl library was not linked, you either cannot use amqps:// connections,
 *  or you have to supply a handle to the openssl library yourself, using the
 *  following method.
 * 
 *  @param  handle          Handle returned by dlopen() that has access to openssl
 */
void openssl(void *handle);
    
/**
 *  End of namespace
 */
}

