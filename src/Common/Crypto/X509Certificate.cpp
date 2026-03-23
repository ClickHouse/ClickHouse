#include <Common/Crypto/X509Certificate.h>

#include <base/scope_guard.h>


#if USE_SSL

namespace DB
{

namespace ErrorCodes
{
extern const int OPENSSL_ERROR;
extern const int BAD_ARGUMENTS;
}

X509Certificate::X509Certificate(X509 * cert_)
    : certificate(cert_)
{
}

X509Certificate::operator X509 *() const
{
    return certificate;
}

X509Certificate & X509Certificate::operator=(X509Certificate && other) noexcept
{
    if (this == &other)
        return *this;

    if (certificate)
        X509_free(certificate);

    certificate = other.certificate;
    other.certificate = nullptr;

    return *this;
}

X509Certificate::X509Certificate(X509Certificate && other) noexcept
{
    certificate = other.certificate;
    other.certificate = nullptr;
}

X509Certificate::X509Certificate(const std::string & path)
{
    BIO_ptr bio(BIO_new(BIO_s_mem()), BIO_free);

    if (!bio)
        throw Exception(ErrorCodes::OPENSSL_ERROR, "BIO_new failed: {}", getOpenSSLErrors());

    BIO * file = BIO_new_file(path.c_str(), "r");

    if (!file)
        throw Exception(ErrorCodes::OPENSSL_ERROR, "BIO_new_file failed: {}", getOpenSSLErrors());

    certificate = PEM_read_bio_X509(file, nullptr, nullptr, nullptr);
    if (!certificate)
        throw Exception(ErrorCodes::OPENSSL_ERROR, "PEM_read_bio_X509 failed for file {}: {}", path, getOpenSSLErrors());
}

X509Certificate::List readCertificatesFromBIO(const BIO_ptr & bio, const std::string & source_description)
{
    X509Certificate::List certs;

    while (true)
    {
        X509 * cert = PEM_read_bio_X509(bio.get(), nullptr, nullptr, nullptr);

        if (!cert)
        {
            auto err = ERR_peek_last_error();
            if (err == 0)
                break;

            /// We read at least one cert, and can't find
            /// the beginning of a next one.
            /// This most likely means we reached the end of the file.
            if (!certs.empty()
                /// Manually unwrap ERR_GET_REASON(err) due to ossl_unused
                /// https://github.com/openssl/openssl/issues/16776
                ///
                /// To be fixed in OpenSSL 3.4+
                && ((err & ERR_SYSTEM_FLAG) == 0 && (err & ERR_REASON_MASK) == PEM_R_NO_START_LINE))
                /// Means we reached the end of the file.
                break;

            throw Exception(
                ErrorCodes::OPENSSL_ERROR, "PEM_read_bio_X509 failed: c:{}, f:{}, e:{}", certs.size(), source_description, getOpenSSLErrors());
        }

        certs.emplace_back(cert);
    }

    if (certs.empty())
        throw Exception(ErrorCodes::OPENSSL_ERROR, "No certificates found in {}", source_description);

    return certs;
}

X509Certificate::List X509Certificate::fromFile(const std::string & path)
{
    BIO_ptr bio(BIO_new_file(path.c_str(), "r"), BIO_free);
    if (!bio)
        throw Exception(ErrorCodes::OPENSSL_ERROR, "BIO_new_file failed: {}", getOpenSSLErrors());

    return readCertificatesFromBIO(bio, path);
}

X509Certificate::List X509Certificate::fromBuffer(const std::string & buffer)
{
    BIO_ptr bio(BIO_new_mem_buf(buffer.c_str(), buffer.size()), BIO_free);
    if (!bio)
        throw Exception(ErrorCodes::OPENSSL_ERROR, "BIO_new_file failed: {}", getOpenSSLErrors());

    return readCertificatesFromBIO(bio, "buffer");
}

X509Certificate::~X509Certificate()
{
    if (certificate)
        X509_free(certificate);
}

uint64_t X509Certificate::version() const
{
    // This is defined by standards (X.509 et al) to be
    // one less than the certificate version.
    // So, eg. a version 3 certificate will return 2.
    return X509_get_version(certificate) + 1;
}

std::string X509Certificate::serialNumber() const
{
    ASN1_INTEGER * serial = X509_get_serialNumber(certificate);
    BIGNUM * bn = ASN1_INTEGER_to_BN(serial, nullptr);

    SCOPE_EXIT({ BN_free(bn); });

    char * hex = BN_bn2hex(bn);
    std::string result(hex);

    SCOPE_EXIT({ OPENSSL_free(hex); });

    return result;
}

std::string X509Certificate::signatureAlgorithm() const
{
    const X509_ALGOR * sig_alg = X509_get0_tbs_sigalg(certificate);
    char buffer[X509Certificate::NAME_BUFFER_SIZE];
    OBJ_obj2txt(buffer, sizeof(buffer), sig_alg->algorithm, 0);
    return buffer;
}

std::string X509Certificate::issuerName() const
{
    char buffer[X509Certificate::NAME_BUFFER_SIZE];
    X509_NAME_oneline(X509_get_issuer_name(certificate), buffer, sizeof(buffer));
    return buffer;
}

std::string X509Certificate::subjectName() const
{
    char buffer[X509Certificate::NAME_BUFFER_SIZE];
    X509_NAME_oneline(X509_get_subject_name(certificate), buffer, sizeof(buffer));
    return buffer;
}

std::string X509Certificate::issuerName(uint nid) const
{
    if (X509_NAME * issuer = X509_get_issuer_name(certificate))
    {
        char buffer[X509Certificate::NAME_BUFFER_SIZE];
        if (X509_NAME_get_text_by_NID(issuer, nid, buffer, sizeof(buffer)) >= 0)
            return std::string(buffer);
    }
    return {};
}

std::string X509Certificate::subjectName(uint nid) const
{
    if (X509_NAME * subj = X509_get_subject_name(certificate))
    {
        char buffer[X509Certificate::NAME_BUFFER_SIZE];
        if (X509_NAME_get_text_by_NID(subj, nid, buffer, sizeof(buffer)) >= 0)
            return std::string(buffer);
    }
    return {};
}

std::string X509Certificate::commonName() const
{
    return subjectName(NID_commonName);
}

std::string X509Certificate::publicKeyAlgorithm() const
{
    EVP_PKEY * pkey = X509_get_pubkey(certificate);
    if (!pkey)
        return {};

    int nid = EVP_PKEY_id(pkey);
    EVP_PKEY_free(pkey);

    if (nid == NID_undef)
        return {};

    char buf[128] = {0};
    OBJ_obj2txt(buf, sizeof(buf), OBJ_nid2obj(nid), 0);
    return std::string(buf);
}

std::string X509Certificate::validFrom() const
{
    ASN1_TIME * valid_from = X509_get_notBefore(certificate);
    return reinterpret_cast<char *>(valid_from->data);
}

std::string X509Certificate::expiresOn() const
{
    ASN1_TIME * not_before = X509_get_notAfter(certificate);
    return reinterpret_cast<char *>(not_before->data);
}

const X509Certificate::Subjects::container & X509Certificate::Subjects::at(Type type_) const
{
    return subjects[static_cast<size_t>(type_)];
}

bool X509Certificate::Subjects::empty()
{
    for (auto & subject_list : subjects)
    {
        if (!subject_list.empty())
            return false;
    }
    return true;
}

void X509Certificate::Subjects::insert(const String & subject_type_, String && subject)
{
    insert(parseSubjectType(subject_type_), std::move(subject));
}

void X509Certificate::Subjects::insert(Type type_, String && subject)
{
    subjects[static_cast<size_t>(type_)].insert(std::move(subject));
}

X509Certificate::Subjects::Type X509Certificate::Subjects::parseSubjectType(const String & type_)
{
    if (type_ == "CN")
        return X509Certificate::Subjects::Type::CN;
    if (type_ == "SAN")
        return X509Certificate::Subjects::Type::SAN;

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown X509 Certificate Subject Type: {}", type_);
}

String X509Certificate::Subjects::toString(X509Certificate::Subjects::Type type_)
{
    switch (type_)
    {
        case X509Certificate::Subjects::Type::CN:
            return "CN";
        case X509Certificate::Subjects::Type::SAN:
            return "SAN";
    }
}

bool X509Certificate::Subjects::operator==(const X509Certificate::Subjects & rhs) const
{
    for (X509Certificate::Subjects::Type type : {X509Certificate::Subjects::Type::CN, X509Certificate::Subjects::Type::SAN})
    {
        if (this->at(type) != rhs.at(type))
            return false;
    }

    return true;
}

X509Certificate::Subjects X509Certificate::extractAllSubjects()
{
    Subjects subjects;

    if (!commonName().empty())
        subjects.insert(Subjects::Type::CN, commonName());

    auto general_names_deleter = [](STACK_OF(GENERAL_NAME) * names) { GENERAL_NAMES_free(names); };

    using GeneralNamesPtr = std::unique_ptr<STACK_OF(GENERAL_NAME), decltype(general_names_deleter)>;
    GeneralNamesPtr cert_names(
        static_cast<STACK_OF(GENERAL_NAME) *>(X509_get_ext_d2i(certificate, NID_subject_alt_name, nullptr, nullptr)),
        general_names_deleter);

    if (!cert_names)
        return subjects;

    const auto * names = reinterpret_cast<const STACK_OF(GENERAL_NAME) *>(cert_names.get());
    uint8_t count = OPENSSL_sk_num(reinterpret_cast<const _STACK *>(names));
    for (uint8_t i = 0; i < count; ++i)
    {
        const GENERAL_NAME * name = static_cast<const GENERAL_NAME *>(OPENSSL_sk_value(reinterpret_cast<const _STACK *>(names), i));

        if (name->type == GEN_DNS || name->type == GEN_URI)
        {
            const ASN1_IA5STRING * ia5 = name->d.ia5;
            const char * data = reinterpret_cast<const char *>(ASN1_STRING_get0_data(ia5));
            std::size_t len = ASN1_STRING_length(ia5);
            if (data && len > 0)
            {
                std::string prefix = (name->type == GEN_DNS) ? "DNS:" : "URI:";
                subjects.insert(Subjects::Type::SAN, prefix + std::string(data, len));
            }
        }
    }

    return subjects;
}

}

#endif
