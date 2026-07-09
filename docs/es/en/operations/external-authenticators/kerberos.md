---
description: 'Los usuarios de ClickHouse existentes y correctamente configurados pueden autenticarse
  mediante el protocolo de autenticación Kerberos.'
slug: /operations/external-authenticators/kerberos
title: 'Kerberos'
doc_type: 'reference'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<div id="kerberos">
  # Kerberos
</div>

<SelfManaged />

Los usuarios existentes de ClickHouse que estén configurados correctamente pueden autenticarse mediante el protocolo de autenticación Kerberos.

Actualmente, Kerberos solo puede utilizarse como autenticador externo para usuarios existentes, definidos en `users.xml` o en rutas locales de control de acceso. Estos usuarios solo pueden usar solicitudes HTTP y deben poder autenticarse mediante el mecanismo GSS-SPNEGO.

Para este enfoque, Kerberos debe estar configurado en el sistema y habilitado en la configuración de ClickHouse.

<div id="enabling-kerberos-in-clickhouse">
  ## Habilitar Kerberos en ClickHouse
</div>

Para habilitar Kerberos, se debe incluir la sección `kerberos` en `config.xml`. Esta sección puede contener parámetros adicionales.

<div id="parameters">
  #### Parámetros
</div>

* `principal` - nombre canónico de la entidad de servicio que se adquirirá y utilizará al aceptar contextos de seguridad.
  * Este parámetro es opcional; si se omite, se usará el principal `default`.

* `realm` - un realm que se utilizará para restringir la autenticación únicamente a las solicitudes cuyo realm del iniciador coincida con él.
  * Este parámetro es opcional; si se omite, no se aplicará ningún filtrado adicional por realm.

* `keytab` - ruta al archivo keytab del servicio.
  * Este parámetro es opcional; si se omite, la ruta al archivo keytab del servicio debe configurarse en la variable de entorno `KRB5_KTNAME`.

Ejemplo (va en `config.xml`):

```xml
<clickhouse>
    <!- ... -->
    <kerberos />
</clickhouse>
```

Con especificación del principal:

```xml
<clickhouse>
    <!- ... -->
    <kerberos>
        <principal>HTTP/clickhouse.example.com@EXAMPLE.COM</principal>
    </kerberos>
</clickhouse>
```

Con filtrado por realm:

```xml
<clickhouse>
    <!- ... -->
    <kerberos>
        <realm>EXAMPLE.COM</realm>
    </kerberos>
</clickhouse>
```

:::note
Solo se puede definir una sección `kerberos`. La presencia de varias secciones `kerberos` hará que ClickHouse deshabilite la autenticación Kerberos.
:::

:::note
Las secciones `principal` y `realm` no se pueden especificar al mismo tiempo. La presencia de ambas secciones hará que ClickHouse deshabilite la autenticación Kerberos.
:::

<div id="kerberos-as-an-external-authenticator-for-existing-users">
  ## Kerberos como autenticador externo para usuarios existentes
</div>

Kerberos puede usarse como método para verificar la identidad de usuarios definidos localmente (usuarios definidos en `users.xml` o en rutas locales de control de acceso). Actualmente, **solo** las solicitudes a través de la interfaz HTTP pueden *autenticarse con Kerberos* (mediante el mecanismo GSS-SPNEGO).

El formato del nombre del principal de Kerberos suele seguir este patrón:

* *primary/instance@REALM*

La parte */instance* puede aparecer cero o más veces. **Se espera que la parte *primary* del nombre principal canónico del iniciador coincida con el nombre de usuario autenticado con Kerberos para que la autenticación se realice correctamente**.

<div id="enabling-kerberos-in-users-xml">
  ### Habilitar Kerberos en `users.xml`
</div>

Para habilitar la autenticación Kerberos para el usuario, especifique la sección `kerberos` en lugar de `password` o de secciones similares en la definición del usuario.

Parámetros:

* `realm` - un realm que se usará para restringir la autenticación únicamente a aquellas solicitudes cuyo realm del iniciador coincida con este.
  * Este parámetro es opcional; si se omite, no se aplicará ningún filtrado adicional por realm.

Ejemplo (va en `users.xml`):

```xml
<clickhouse>
    <!- ... -->
    <users>
        <!- ... -->
        <my_user>
            <!- ... -->
            <kerberos>
                <realm>EXAMPLE.COM</realm>
            </kerberos>
        </my_user>
    </users>
</clickhouse>
```

:::note
Tenga en cuenta que la autenticación Kerberos no se puede usar junto con ningún otro mecanismo de autenticación. La presencia de cualquier otra sección, como `password`, junto con `kerberos` hará que ClickHouse se apague.
:::

:::info Reminder
Tenga en cuenta que, a partir de ahora, una vez que el usuario `my_user` use `kerberos`, Kerberos debe estar habilitado en el archivo principal `config.xml`, como se describió anteriormente.
:::

<div id="enabling-kerberos-using-sql">
  ### Habilitar Kerberos mediante SQL
</div>

Cuando está habilitado en ClickHouse el [control de acceso y la gestión de cuentas basados en SQL](/es/operations/access-rights#access-control-usage), también se pueden crear mediante sentencias SQL usuarios identificados con Kerberos.

```sql
CREATE USER my_user IDENTIFIED WITH kerberos REALM 'EXAMPLE.COM'
```

...o, sin filtrar por el realm:

```sql
CREATE USER my_user IDENTIFIED WITH kerberos
```