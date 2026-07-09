---
description: 'Документация по SSL-сертификатам X.509'
slug: /operations/external-authenticators/ssl-x509
title: 'Аутентификация по SSL-сертификату X.509'
doc_type: 'reference'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

[Параметр SSL &#39;strict&#39;](../server-configuration-parameters/settings.md#openssl) включает обязательную проверку сертификатов для входящих соединений. В этом случае можно устанавливать только соединения с доверенными сертификатами. Соединения с недоверенными сертификатами будут отклоняться. Таким образом, проверка сертификатов позволяет однозначно аутентифицировать входящее соединение. Для идентификации подключенного пользователя используется поле сертификата `Common Name` или `subjectAltName extension`. `subjectAltName extension` поддерживает использование одного подстановочного символа &#39;*&#39; в конфигурации сервера. Это позволяет сопоставить несколько сертификатов с одним и тем же пользователем. Кроме того, перевыпуск и отзыв сертификатов не влияют на конфигурацию ClickHouse.

Чтобы включить аутентификацию по SSL-сертификату, в файле настроек `users.xml ` для каждого пользователя ClickHouse должен быть указан список `Common Name` или `Subject Alt Name`:

**Пример**

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

Чтобы [`цепочка доверия`](https://en.wikipedia.org/wiki/Chain_of_trust) SSL работала корректно, также важно убедиться, что параметр [`caConfig`](../server-configuration-parameters/settings.md#openssl) настроен правильно.