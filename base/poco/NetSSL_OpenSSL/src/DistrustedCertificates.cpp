//
// DistrustedCertificates.cpp
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  DistrustedCertificates
//
// Copyright (c) 2026, ClickHouse, Inc.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/DistrustedCertificates.h"
#include <algorithm>
#include <array>
#include <openssl/evp.h>
#include <openssl/x509_vfy.h>


namespace
{
	using SPKIHash = std::array<unsigned char, 32>;

	consteval unsigned char hexDigit(char c)
	{
		if (c >= '0' && c <= '9')
			return c - '0';
		if (c >= 'A' && c <= 'F')
			return c - 'A' + 10;
		throw "Invalid hex digit";
	}

	consteval SPKIHash fromHex(const char (&hex)[65])
	{
		SPKIHash result{};
		for (size_t i = 0; i < result.size(); ++i)
			result[i] = static_cast<unsigned char>(hexDigit(hex[2 * i]) << 4 | hexDigit(hex[2 * i + 1]));
		return result;
	}

	/// SHA-256 of the DER-encoded `SubjectPublicKeyInfo` of every distrusted key.
	constexpr SPKIHash distrusted_keys[] =
	{
		/// eMudhra: the private keys of the intermediate CAs below, issued in 2025, can be recovered
		/// from their certificates. Each of them is issued by `emSign Root TLS CA - G3` or `emSign Root CS CA - G3`
		/// and cross-signed by Sectigo `AAA Certificate Services`; the cross-signed certificates have the same keys.
		/// The roots that issued or cross-signed them are distrusted as well.
		/// https://emudhra.we-have-a-problem.com/
		fromHex("EBED6475C2C2D80FCB199F3FE8DD6E6B3CE60B77EB15D4E42C3DEC0C6F5B5105"), /// EM DV TLS CA - G3B
		fromHex("43BFFA19D6EF651451F6841E019931E96F6F2EAEFF28AB4B6F1CE4576DE2717B"), /// EM OV TLS CA - G3B
		fromHex("E0254C7FCDC265E2F0F8A428DABB75AB4AE039B4872D62F5EE57F0877D5A8A41"), /// EM EV TLS CA - G3B
		fromHex("49DD353DC2B919A3770645117D0E60C16115E7D1BC259EEFE734B00EA434FE5F"), /// EM DV TLS CA - G3C
		fromHex("63ECEFEE431D49523BC1136C4E72D0206A477086CAF606CD5CF025686C109CF8"), /// EM OV TLS CA - G3C
		fromHex("5C0660A2B164910D8FF589C18E14552E9E0EC237DCEFE2CB736D41A4811F81A0"), /// EM EV TLS CA - G3C
		fromHex("EC4FFA1AD0AC88081314B7AAFFEF6A91F97CD57291BB5BA686C5415F5EB4F6B6"), /// EM OV CS CA - G3B
		fromHex("92C063D67E8F14DB71D288E11266FFDAF69CBF115F7BAB14F3BC2F68D25E0E30"), /// EM OV CS CA - G3C
		fromHex("C619E4780ED8E0ABEDA24AD732567DE4383A7EA941338F6D1A7D1B49AC792FE9"), /// emSign Root TLS CA - G3
		fromHex("0A6C2B2CA5B364207954171987AC9D2178561569D8205457E77CA90C32EDE282"), /// emSign Root CS CA - G3
		fromHex("8D417DB2DD8BF5E3084D1E3F196D583849D81BDD4C00C70B9D39369E96B8C782"), /// emSign ECC Root CA - G3
	};
}


namespace Poco {
namespace Net {


int checkDistrustedCertificate(X509 * certificate)
{
	X509_PUBKEY * key = X509_get_X509_PUBKEY(certificate);
	if (key == nullptr)
		return X509_V_ERR_UNSPECIFIED;

	/// Not `X509_pubkey_digest`: it hashes only the key bits, without the algorithm identifier.
	unsigned char * der = nullptr;
	int der_size = i2d_X509_PUBKEY(key, &der);
	if (der_size <= 0)
		return X509_V_ERR_OUT_OF_MEM;

	SPKIHash hash;
	unsigned int hash_size = 0;
	int ok = EVP_Digest(der, der_size, hash.data(), &hash_size, EVP_sha256(), nullptr);
	OPENSSL_free(der);
	if (ok != 1 || hash_size != hash.size())
		return X509_V_ERR_UNSPECIFIED;

	if (std::find(std::begin(distrusted_keys), std::end(distrusted_keys), hash) != std::end(distrusted_keys))
		return X509_V_ERR_CERT_REVOKED;

	return X509_V_OK;
}


} } // namespace Poco::Net
