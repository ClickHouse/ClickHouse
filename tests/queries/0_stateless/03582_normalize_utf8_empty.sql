-- Tags: no-fasttest

SELECT '' AS value,
    normalizeUTF8NFC(value) AS nfc, length(nfc) AS nfc_len,
    normalizeUTF8NFD(value) AS nfd, length(nfd) AS nfd_len,
    normalizeUTF8NFKC(value) AS nfkc, length(nfkc) AS nfkc_len,
    normalizeUTF8NFKD(value) AS nfkd, length(nfkd) AS nfkd_len;
