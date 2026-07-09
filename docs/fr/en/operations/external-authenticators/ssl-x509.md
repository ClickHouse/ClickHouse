---
description: 'Documentation sur SSL X.509'
slug: /operations/external-authenticators/ssl-x509
title: 'Authentification par certificat SSL X.509'
doc_type: 'reference'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

L’[option SSL &#39;strict&#39;](../server-configuration-parameters/settings.md#openssl) active la validation obligatoire des certificats pour les connexions entrantes. Dans ce cas, seules les connexions dotées de certificats de confiance peuvent être établies. Les connexions avec des certificats non approuvés seront rejetées. Ainsi, la validation des certificats permet d’authentifier de manière univoque une connexion entrante. Le champ `Common Name` ou l’`extension subjectAltName` du certificat est utilisé pour identifier l’utilisateur connecté. L’`extension subjectAltName` prend en charge l’utilisation d’un caractère générique &#39;*&#39; dans la configuration du serveur. Cela permet d’associer plusieurs certificats à un même utilisateur. De plus, la réémission et la révocation des certificats n’affectent pas la configuration de ClickHouse.

Pour activer l’authentification par certificat SSL, une liste de `Common Name` ou de `Subject Alt Name` doit être spécifiée pour chaque utilisateur ClickHouse dans le fichier de configuration `users.xml ` :

**Exemple**

```xml
<clickhouse>
    <!- ... -->
    <users>
        <user_name_1>
            <ssl_certificates>
                <common_name>host.domain.com:example_user</common_name>
                <common_name>host.domain.com:example_user_dev</common_name>
                <!-- More names -->
            </ssl_certificates>
            <!-- Other settings -->
        </user_name_1>
        <user_name_2>
            <ssl_certificates>
                <subject_alt_name>DNS:host.domain.com</subject_alt_name>
                <!-- More names -->
            </ssl_certificates>
            <!-- Other settings -->
        </user_name_2>
        <user_name_3>
            <ssl_certificates>
                <!-- Wildcard support -->
                <subject_alt_name>URI:spiffe://foo.com/*/bar</subject_alt_name>
            </ssl_certificates>
        </user_name_3>
    </users>
</clickhouse>
```

Pour que la [`chaîne de confiance`](https://en.wikipedia.org/wiki/Chain_of_trust) SSL fonctionne correctement, il est également important de vérifier que le paramètre [`caConfig`](../server-configuration-parameters/settings.md#openssl) est correctement configuré.