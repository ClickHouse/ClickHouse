---
description: 'Documentação sobre SSL X.509'
slug: /operations/external-authenticators/ssl-x509
title: 'Autenticação por certificado SSL X.509'
doc_type: 'reference'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

A [opção SSL &#39;strict&#39;](../server-configuration-parameters/settings.md#openssl) habilita a validação obrigatória de certificados para conexões de entrada. Nesse caso, só podem ser estabelecidas conexões com certificados confiáveis. Conexões com certificados não confiáveis serão rejeitadas. Assim, a validação de certificados permite autenticar de forma exclusiva uma conexão de entrada. O campo `Common Name` ou `subjectAltName extension` do certificado é usado para identificar o usuário conectado. `subjectAltName extension` oferece suporte ao uso de um caractere curinga &#39;*&#39; na configuração do servidor. Isso permite associar vários certificados ao mesmo usuário. Além disso, a reemissão e a revogação de certificados não afetam a configuração do ClickHouse.

Para habilitar a autenticação por certificado SSL, é preciso especificar no arquivo de configuração `users.xml ` uma lista de `Common Name`&#39;s ou `Subject Alt Name`&#39;s para cada usuário do ClickHouse:

**Exemplo**

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

Para que o [`cadeia de confiança`](https://en.wikipedia.org/wiki/Chain_of_trust) do SSL funcione corretamente, também é importante garantir que o parâmetro [`caConfig`](../server-configuration-parameters/settings.md#openssl) esteja configurado corretamente.