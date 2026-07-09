---
description: 'Documentación de SSL X.509'
slug: /operations/external-authenticators/ssl-x509
title: 'Autenticación por certificado SSL X.509'
doc_type: 'reference'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<SelfManaged />

La [opción SSL &#39;strict&#39;](../server-configuration-parameters/settings.md#openssl) habilita la validación obligatoria de certificados para las conexiones entrantes. En este caso, solo pueden establecerse conexiones con certificados de confianza. Las conexiones con certificados no confiables se rechazarán. Por lo tanto, la validación de certificados permite autenticar de forma inequívoca una conexión entrante. El campo `Common Name` o la extensión `subjectAltName` del certificado se utiliza para identificar al usuario conectado. La extensión `subjectAltName` admite el uso de un comodín &#39;*&#39; en la configuración del servidor. Esto permite asociar varios certificados al mismo usuario. Además, la reemisión y revocación de los certificados no afectan a la configuración de ClickHouse.

Para habilitar la autenticación mediante certificado SSL, debe especificarse una lista de `Common Name` o `Subject Alt Name` para cada usuario de ClickHouse en el archivo de configuración `users.xml `:

**Ejemplo**

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

Para que la [`cadena de confianza`](https://en.wikipedia.org/wiki/Chain_of_trust) de SSL funcione correctamente, también es importante asegurarse de que el parámetro [`caConfig`](../server-configuration-parameters/settings.md#openssl) esté bien configurado.