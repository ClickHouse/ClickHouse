---
slug: /en/operations/external-authenticators/totp
title: "TOTP"
---
import SelfManaged from '@site/docs/en/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

Time-Based One-Time Password (TOTP) can be used to authenticate ClickHouse users by generating temporary access codes that are valid for a limited time.
In current implementation it is a standalone authentication method, rather than a second factor for password-based authentication.
This TOTP authentication method aligns with [RFC 6238](https://datatracker.ietf.org/doc/html/rfc6238) standards, making it compatible with popular TOTP applications like Google Authenticator, 1Password and similar tools.

## TOTP Authentication Configuration {#totp-auth-configuration}

To enable TOTP authentication for a user, configure the `time_based_one_time_password` section in `users.xml`. This section defines the TOTP settings, such as secret, validity period, number of digits, and hash algorithm.

**Example**
```xml
<clickhouse>
    <!-- ... -->
    <users>
        <my_user>
            <!-- TOTP authentication configuration -->
            <time_based_one_time_password>
                <secret>JBSWY3DPEHPK3PXP</secret>      <!-- Base32-encoded TOTP secret -->
                <period>30</period>                    <!-- Optional: OTP validity period in seconds -->
                <digits>6</digits>                     <!-- Optional: Number of digits in the OTP -->
                <algorithm>SHA1</algorithm>            <!-- Optional: Hash algorithm: SHA1, SHA256, SHA512 -->
            </time_based_one_time_password>
        </my_user>
    </users>
</clickhouse>

Parameters:

- secret - (Required) The base32-encoded secret key used to generate TOTP codes.
- period - Optional. Sets the validity period of each OTP in seconds. Must be a positive number not exceeding 120. Default is 30.
- digits - Optional. Specifies the number of digits in each OTP. Must be between 4 and 10. Default is 6.
- algorithm - Optional. Defines the hash algorithm for generating OTPs. Supported values are SHA1, SHA256, and SHA512. Default is SHA1.

Generating a TOTP Secret

To generate a TOTP-compatible secret for use with ClickHouse, run the following command in the terminal:

```bash
$ base32 -w32 < /dev/urandom | head -1
```

This command will produce a base32-encoded secret that can be added to the secret field in users.xml.

To enable TOTP for a specific user, replace any existing password-based fields (like `password` or `password_sha256_hex`) with the `time_based_one_time_password` section. Only one authentication method is allowed per user, so TOTP cannot be combined with other methods such as password or LDAP.

## Enabling TOTP Authentication using SQL {#enabling-totp-auth-using-sql}

When SQL-driven Access Control and Account Management is enabled, TOTP authentication can be set for users via SQL:

```SQL
CREATE USER my_user IDENTIFIED WITH one_time_password BY 'JBSWY3DPEHPK3PXP';
```

Values for `period`, `digits`, and `algorithm` will be set to their default values.
