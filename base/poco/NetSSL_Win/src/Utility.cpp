//
// Utility.cpp
//
// Library: NetSSL_Win
// Package: SSLCore
// Module:  Utility
//
// Copyright (c) 2006-2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/Utility.h"
#include "Poco/String.h"
#include "Poco/NumberFormatter.h"
#include "Poco/Util/OptionException.h"
#include <windows.h>
#include <winerror.h>


namespace Poco {
namespace Net {


Poco::FastMutex Utility::_mutex;


Context::VerificationMode Utility::convertVerificationMode(const std::string& vMode)
{
	std::string mode = Poco::toLower(vMode);
	Context::VerificationMode verMode = Context::VERIFY_STRICT;

	if (mode == "none")
		verMode = Context::VERIFY_NONE;
	else if (mode == "relaxed")
		verMode = Context::VERIFY_RELAXED;
	else if (mode == "strict")
		verMode = Context::VERIFY_STRICT;
	else
		throw Poco::Util::OptionException(std::string("Wrong value >") + vMode + std::string("< for a verificationMode. Can only be none, relaxed, strict or once."));

	return verMode;
}


inline void add(std::map<long, const std::string>& messageMap, long key, const std::string& val)
{
	messageMap.insert(std::make_pair(key, val));
}


std::map<long, const std::string> Utility::initSSPIErr()
{
	std::map<long, const std::string> messageMap;
	add(messageMap, NTE_BAD_UID, "Bad UID");
	add(messageMap, NTE_BAD_HASH, "Bad Hash");
	add(messageMap, NTE_BAD_KEY, "Bad Key");
	add(messageMap, NTE_BAD_LEN, "Bad Length");
	add(messageMap, NTE_BAD_DATA, "Bad Data");
	add(messageMap, NTE_BAD_SIGNATURE, "Invalid signature");
	add(messageMap, NTE_BAD_VER, "Bad Version of provider");
	add(messageMap, NTE_BAD_ALGID, "Invalid algorithm specified");
	add(messageMap, NTE_BAD_FLAGS, "Invalid flags specified");
	add(messageMap, NTE_BAD_TYPE, "Invalid type specified");
	add(messageMap, NTE_BAD_KEY_STATE, "Key not valid for use in specified state");
	add(messageMap, NTE_BAD_HASH_STATE, "Hash not valid for use in specified state");
	add(messageMap, NTE_NO_KEY, "Key does not exist");
	add(messageMap, NTE_NO_MEMORY, "Insufficient memory available for the operation");
	add(messageMap, NTE_EXISTS, "Object already exists");
	add(messageMap, NTE_PERM, "Permission denied");
	add(messageMap, NTE_NOT_FOUND, "Object was not found");
	add(messageMap, NTE_DOUBLE_ENCRYPT, "Data already encrypted");
	add(messageMap, NTE_BAD_PROVIDER, "Invalid provider specified");
	add(messageMap, NTE_BAD_PROV_TYPE, "Invalid provider type specified");
	add(messageMap, NTE_BAD_PUBLIC_KEY, "Provider's public key is invalid");
	add(messageMap, NTE_BAD_KEYSET, "Keyset does not exist");
	add(messageMap, NTE_PROV_TYPE_NOT_DEF, "Provider type not defined");
	add(messageMap, NTE_PROV_TYPE_ENTRY_BAD, "Provider type as registered is invalid");
	add(messageMap, NTE_KEYSET_NOT_DEF, "The keyset is not defined");
	add(messageMap, NTE_KEYSET_ENTRY_BAD, "Keyset as registered is invalid");
	add(messageMap, NTE_PROV_TYPE_NO_MATCH, "Provider type does not match registered value");
	add(messageMap, NTE_SIGNATURE_FILE_BAD, "The digital signature file is corrupt");
	add(messageMap, NTE_PROVIDER_DLL_FAIL, "Provider DLL failed to initialize correctly");
	add(messageMap, NTE_PROV_DLL_NOT_FOUND, "Provider DLL could not be found");
	add(messageMap, NTE_BAD_KEYSET_PARAM, "The Keyset parameter is invalid");
	add(messageMap, NTE_FAIL, "NTE_FAIL: An internal error occurred");
	add(messageMap, NTE_SYS_ERR, "NTE_SYS_ERR: A base error occurred");
	add(messageMap, NTE_SILENT_CONTEXT, "Provider could not perform the action since the context was acquired as silent");
	add(messageMap, NTE_TOKEN_KEYSET_STORAGE_FULL, "The security token does not have storage space available for an additional container");
	add(messageMap, NTE_TEMPORARY_PROFILE, "The profile for the user is a temporary profile");
	add(messageMap, NTE_FIXEDPARAMETER, "The key parameters could not be set because the CSP uses fixed parameters");
	add(messageMap, SEC_E_INSUFFICIENT_MEMORY, "Not enough memory is available to complete this request");
	add(messageMap, SEC_E_INVALID_HANDLE, "The handle specified is invalid");
	add(messageMap, SEC_E_UNSUPPORTED_FUNCTION, "The function requested is not supported");
	add(messageMap, SEC_E_TARGET_UNKNOWN, "The specified target is unknown or unreachable");
	add(messageMap, SEC_E_INTERNAL_ERROR, "The Local Security Authority cannot be contacted");
	add(messageMap, SEC_E_SECPKG_NOT_FOUND, "The requested security package does not exist");
	add(messageMap, SEC_E_NOT_OWNER, "The caller is not the owner of the desired credentials");
	add(messageMap, SEC_E_CANNOT_INSTALL, "The security package failed to initialize, and cannot be installed");
	add(messageMap, SEC_E_INVALID_TOKEN, "The token supplied to the function is invalid");
	add(messageMap, SEC_E_CANNOT_PACK, "The security package is not able to marshall the logon buffer, so the logon attempt has failed");
	add(messageMap, SEC_E_QOP_NOT_SUPPORTED, "The per-message Quality of Protection is not supported by the security package");
	add(messageMap, SEC_E_NO_IMPERSONATION, "The security context does not allow impersonation of the client");
	add(messageMap, SEC_E_LOGON_DENIED, "The logon attempt failed");
	add(messageMap, SEC_E_UNKNOWN_CREDENTIALS, "The credentials supplied to the package were not recognized");
	add(messageMap, SEC_E_NO_CREDENTIALS, "No credentials are available in the security package");
	add(messageMap, SEC_E_MESSAGE_ALTERED, "The message or signature supplied for verification has been altered");
	add(messageMap, SEC_E_OUT_OF_SEQUENCE, "The message supplied for verification is out of sequence");
	add(messageMap, SEC_E_NO_AUTHENTICATING_AUTHORITY, "No authority could be contacted for authentication");
	add(messageMap, SEC_I_CONTINUE_NEEDED, "The function completed successfully, but must be called again to complete the context");
	add(messageMap, SEC_I_COMPLETE_NEEDED, "The function completed successfully, but CompleteToken must be called");
	add(messageMap, SEC_I_COMPLETE_AND_CONTINUE, "The function completed successfully, but both CompleteToken and this function must be called to complete the context");
	add(messageMap, SEC_I_LOCAL_LOGON, "The logon was completed, but no network authority was available. The logon was made using locally known information");
	add(messageMap, SEC_E_BAD_PKGID, "The requested security package does not exist");
	add(messageMap, SEC_E_CONTEXT_EXPIRED, "The context has expired and can no longer be used");
	add(messageMap, SEC_E_INCOMPLETE_MESSAGE, "The supplied message is incomplete. The signature was not verified");
	add(messageMap, SEC_E_INCOMPLETE_CREDENTIALS, "The credentials supplied were not complete, and could not be verified. The context could not be initialized");
	add(messageMap, SEC_E_BUFFER_TOO_SMALL, "The buffers supplied to a function was too small");
	add(messageMap, SEC_I_RENEGOTIATE, "The context data must be renegotiated with the peer");
	add(messageMap, SEC_E_WRONG_PRINCIPAL, "The target principal name is incorrect");
	add(messageMap, SEC_I_NO_LSA_CONTEXT, "There is no LSA mode context associated with this context");
	add(messageMap, SEC_E_TIME_SKEW, "The clocks on the client and server machines are skewed");
	add(messageMap, SEC_E_UNTRUSTED_ROOT, "The certificate chain was issued by an authority that is not trusted");
	add(messageMap, SEC_E_ILLEGAL_MESSAGE, "The message received was unexpected or badly formatted");
	add(messageMap, SEC_E_CERT_UNKNOWN, "An unknown error occurred while processing the certificate");
	add(messageMap, SEC_E_CERT_EXPIRED, "The received certificate has expired");
	add(messageMap, SEC_E_ENCRYPT_FAILURE, "The specified data could not be encrypted");
	add(messageMap, SEC_E_DECRYPT_FAILURE, "The specified data could not be decrypted");
	add(messageMap, SEC_E_ALGORITHM_MISMATCH, "The client and server cannot communicate, because they do not possess a common algorithm");
	add(messageMap, SEC_E_SECURITY_QOS_FAILED, "The security context could not be established due to a failure in the requested quality of service (e.g. mutual authentication or delegation)");
	add(messageMap, SEC_E_UNFINISHED_CONTEXT_DELETED, "A security context was deleted before the context was completed.  This is considered a logon failure");
	add(messageMap, SEC_E_NO_TGT_REPLY, "The client is trying to negotiate a context and the server requires user-to-user but didn't send a TGT reply");
	add(messageMap, SEC_E_NO_IP_ADDRESSES, "Unable to accomplish the requested task because the local machine does not have any IP addresses");
	add(messageMap, SEC_E_WRONG_CREDENTIAL_HANDLE, "The supplied credential handle does not match the credential associated with the security context");
	add(messageMap, SEC_E_CRYPTO_SYSTEM_INVALID, "The crypto system or checksum function is invalid because a required function is unavailable");
	add(messageMap, SEC_E_MAX_REFERRALS_EXCEEDED, "The number of maximum ticket referrals has been exceeded");
	add(messageMap, SEC_E_MUST_BE_KDC, "The local machine must be a Kerberos KDC (domain controller) and it is not");
	add(messageMap, SEC_E_STRONG_CRYPTO_NOT_SUPPORTED, "The other end of the security negotiation is requires strong crypto but it is not supported on the local machine");
	add(messageMap, SEC_E_TOO_MANY_PRINCIPALS, "The KDC reply contained more than one principal name");
	add(messageMap, SEC_E_NO_PA_DATA, "Expected to find PA data for a hint of what type to use, but it was not found");
	//80092001
	add(messageMap, CRYPT_E_SELF_SIGNED, "The specified certificate is self signed");
	add(messageMap, CRYPT_E_DELETED_PREV, "The previous certificate or CRL context was deleted");
	add(messageMap, CRYPT_E_NO_MATCH, "Cannot find the requested object");
	add(messageMap, CRYPT_E_UNEXPECTED_MSG_TYPE, "The certificate does not have a property that references a private key");
	add(messageMap, CRYPT_E_NO_KEY_PROPERTY, "Cannot find the certificate and private key for decryption");
	add(messageMap, CRYPT_E_NO_DECRYPT_CERT, "Cannot find the certificate and private key to use for decryption");
	add(messageMap, CRYPT_E_BAD_MSG, "Not a cryptographic message or the cryptographic message is not formatted correctly");
	add(messageMap, CRYPT_E_NO_SIGNER, "The signed cryptographic message does not have a signer for the specified signer index");
	add(messageMap, CRYPT_E_PENDING_CLOSE, "Final closure is pending until additional frees or closes");
	add(messageMap, CRYPT_E_REVOKED, "The certificate is revoked");
	add(messageMap, CRYPT_E_NO_REVOCATION_DLL, "No Dll or exported function was found to verify revocation");
	add(messageMap, CRYPT_E_NO_REVOCATION_CHECK, "The revocation function was unable to check revocation for the certificate");
	add(messageMap, CRYPT_E_REVOCATION_OFFLINE, "The revocation function was unable to check revocation because the revocation server was offline");
	add(messageMap, CRYPT_E_NOT_IN_REVOCATION_DATABASE, "The certificate is not in the revocation server's database");
	add(messageMap, CRYPT_E_INVALID_NUMERIC_STRING, "The string contains a non-numeric character");
	add(messageMap, CRYPT_E_INVALID_PRINTABLE_STRING, "The string contains a non-printable character");
	add(messageMap, CRYPT_E_INVALID_IA5_STRING, "The string contains a character not in the 7 bit ASCII character set");
	add(messageMap, CRYPT_E_INVALID_X500_STRING, "The string contains an invalid X500 name attribute key, oid, value or delimiter");
	add(messageMap, CRYPT_E_NOT_CHAR_STRING, "The dwValueType for the CERT_NAME_VALUE is not one of the character strings.  Most likely it is either a CERT_RDN_ENCODED_BLOB or CERT_TDN_OCTED_STRING");
	add(messageMap, CRYPT_E_FILERESIZED, "The Put operation can not continue.  The file needs to be resized.  However, there is already a signature present.  A complete signing operation must be done");
	add(messageMap, CRYPT_E_SECURITY_SETTINGS, "The cryptographic operation failed due to a local security option setting");
	add(messageMap, CRYPT_E_NO_VERIFY_USAGE_DLL, "No DLL or exported function was found to verify subject usage");
	add(messageMap, CRYPT_E_NO_VERIFY_USAGE_CHECK, "The called function was unable to do a usage check on the subject");
	add(messageMap, CRYPT_E_VERIFY_USAGE_OFFLINE, "Since the server was offline, the called function was unable to complete the usage check");
	add(messageMap, CRYPT_E_NOT_IN_CTL, "The subject was not found in a Certificate Trust List (CTL)");
	add(messageMap, CRYPT_E_NO_TRUSTED_SIGNER, "None of the signers of the cryptographic message or certificate trust list is trusted");
	add(messageMap, CRYPT_E_MISSING_PUBKEY_PARA, "The public key's algorithm parameters are missing");
	add(messageMap, TRUST_E_CERT_SIGNATURE, "The signature of the certificate cannot be verified.");
	add(messageMap, TRUST_E_BASIC_CONSTRAINTS, "The basic constraints of the certificate are not valid or missing");
	add(messageMap, CERT_E_UNTRUSTEDROOT, "A certification chain processed correctly but terminated in a root certificate not trusted by the trust provider");
	add(messageMap, CERT_E_UNTRUSTEDTESTROOT, "The root certificate is a testing certificate and policy settings disallow test certificates");
	add(messageMap, CERT_E_CHAINING, "A chain of certificates was not correctly created");
	add(messageMap, CERT_E_WRONG_USAGE, "The certificate is not valid for the requested usage");
	add(messageMap, CERT_E_EXPIRED, "A required certificate is not within its validity period");
	add(messageMap, CERT_E_VALIDITYPERIODNESTING, "The validity periods of the certification chain do not nest correctly");
	add(messageMap, CERT_E_PURPOSE, "A certificate is being used for a purpose that is not supported");
	add(messageMap, CERT_E_ROLE, "A certificate that can only be used as an end entity is being used as a CA or visa versa");
	add(messageMap, CERT_E_CN_NO_MATCH, "The CN name of the certificate does not match the passed value");
	add(messageMap, CERT_E_REVOKED, "A certificate in the chain has been explicitly revoked by its issuer");
	add(messageMap, CERT_E_REVOCATION_FAILURE, "The revocation process could not continue. The certificates could not be checked");
	return messageMap;
}


const std::string& Utility::formatError(long errCode)
{
	Poco::FastMutex::ScopedLock lock(_mutex);

	static const std::string def("Internal SSPI error");
	static const std::map<long, const std::string> errs(initSSPIErr());

	const std::map<long, const std::string>::const_iterator it = errs.find(errCode);
	if (it != errs.end())
		return it->second;
	else
		return def;
}


} } // namespace Poco::Net
