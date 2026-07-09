---
description: 'Les protocoles composables offrent une configuration plus souple de l’accès TCP
  au serveur ClickHouse.'
sidebar_label: 'Protocoles composables'
sidebar_position: 64
slug: /operations/settings/composable-protocols
title: 'Protocoles composables'
doc_type: 'reference'
---

<div id="overview">
  ## Vue d’ensemble
</div>

Les protocoles composables permettent de configurer de façon plus flexible l’accès TCP au
serveur ClickHouse. Cette configuration peut coexister avec la configuration
classique ou la remplacer.

<div id="composable-protocols-section-is-denoted-as-protocols-in-configuration-xml">
  ## Configuration des protocoles composables
</div>

Les protocoles composables peuvent être configurés dans un fichier de configuration XML. La section consacrée aux protocoles
est délimitée par les balises `protocols` dans le fichier de configuration XML :

```xml
<protocols>

</protocols>
```

<div id="basic-modules-define-protocol-layers">
  ### Configuration des couches de protocole
</div>

Vous pouvez définir des couches de protocole à l’aide de modules de base. Par exemple, pour définir une
couche HTTP, vous pouvez ajouter un nouveau module de base dans la section `protocols` :

```xml
<protocols>

  <!-- plain_http module -->
  <plain_http>
    <type>http</type>
  </plain_http>

</protocols>
```

Les modules peuvent être configurés comme suit :

* `plain_http` - nom auquel une autre couche peut faire référence
* `type` - indique le gestionnaire de protocole qui sera instancié pour traiter les données.
  Il existe l’ensemble suivant de gestionnaires de protocole prédéfinis :
  * `tcp` - gestionnaire du protocole natif ClickHouse
  * `http` - gestionnaire du protocole HTTP ClickHouse
  * `tls` - couche de chiffrement TLS
  * `proxy1` - couche PROXYv1
  * `mysql` - gestionnaire du protocole de compatibilité MySQL
  * `postgres` - gestionnaire du protocole de compatibilité PostgreSQL
  * `prometheus` - gestionnaire du protocole Prometheus
  * `interserver` - gestionnaire interserver ClickHouse

:::note
Le gestionnaire du protocole `gRPC` n’est pas implémenté pour `protocole composable`
:::

<div id="endpoint-ie-listening-port-is-denoted-by-port-and-optional-host-tags">
  ### Configuration des points de terminaison
</div>

Les points de terminaison (ports d’écoute) sont indiqués par la balise `<port>` et, en option, la balise `<host>`.
Par exemple, pour configurer un point de terminaison sur la couche HTTP ajoutée précédemment, nous
pourrions modifier notre configuration comme suit :

```xml
<protocols>

  <plain_http>

    <type>http</type>
    <!-- endpoint -->
    <host>127.0.0.1</host>
    <port>8123</port>

  </plain_http>

</protocols>
```

Si la balise `<host>` est omise, la balise `<listen_host>` de la config racine est
utilisée.

<div id="layers-sequence-is-defined-by-impl-tag-referencing-another-module">
  ### Configuration des séquences de couches
</div>

Les séquences de couches sont définies à l’aide de la balise `<impl>` et font référence à un autre
module. Par exemple, pour configurer une couche TLS par-dessus notre module plain&#95;http,
nous pouvons modifier davantage notre configuration comme suit :

```xml
<protocols>

  <!-- http module -->
  <plain_http>
    <type>http</type>
  </plain_http>

  <!-- https module configured as a tls layer on top of plain_http module -->
  <https>
    <type>tls</type>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8443</port>
  </https>

</protocols>
```

<div id="endpoint-can-be-attached-to-any-layer">
  ### Associer des points de terminaison aux couches
</div>

Des points de terminaison peuvent être associés à n’importe quelle couche. Par exemple, nous pouvons définir des points de terminaison pour
HTTP (port 8123) et HTTPS (port 8443) :

```xml
<protocols>

  <plain_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8123</port>
  </plain_http>

  <https>
    <type>tls</type>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8443</port>
  </https>

</protocols>
```

<div id="additional-endpoints-can-be-defined-by-referencing-any-module-and-omitting-type-tag">
  ### Définition de points de terminaison supplémentaires
</div>

Des points de terminaison supplémentaires peuvent être définis en faisant référence à n’importe quel module et en omettant la
balise `<type>`. Par exemple, nous pouvons définir le point de terminaison `another_http` pour le
module `plain_http` comme suit :

```xml
<protocols>

  <plain_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8123</port>
  </plain_http>

  <https>
    <type>tls</type>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8443</port>
  </https>

  <another_http>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8223</port>
  </another_http>

</protocols>
```

<div id="custom-http-handlers-per-endpoint">
  ### Gestionnaires HTTP personnalisés par point de terminaison
</div>

Par défaut, toutes les entrées du protocole `type=http` partagent la même configuration
`<http_handlers>`. Vous pouvez remplacer ce comportement en ajoutant une balise `<handlers>` qui pointe
vers une autre section de configuration. Cela permet à chaque port HTTP d’appliquer un
ensemble différent de règles de routage HTTP.

Par exemple, pour exécuter une API HTTP alternative sur le port 8124 avec ses propres gestionnaires :

```xml
<protocols>

  <plain_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8123</port>
  </plain_http>

  <alt_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8124</port>
    <handlers>http_handlers_alt</handlers>
  </alt_http>

</protocols>

<!-- Default handlers used by plain_http (port 8123) -->
<http_handlers>
    <defaults/>
</http_handlers>

<!-- Alternative handlers used by alt_http (port 8124) -->
<http_handlers_alt>
    <rule>
        <url>/custom</url>
        <handler>
            <type>predefined_query_handler</type>
            <query>SELECT 'custom_endpoint'</query>
        </handler>
    </rule>
    <defaults/>
</http_handlers_alt>
```

Dans cet exemple, les requêtes vers le port 8123 utilisent les règles standard `<http_handlers>`,
tandis que les requêtes vers le port 8124 utilisent les règles `<http_handlers_alt>`. Si `<handlers>`
est omis, le point de terminaison utilise par défaut `<http_handlers>`.

La section des gestionnaires personnalisés suit le même format que
[`<http_handlers>`](/fr/docs/operations/server-configuration-parameters/settings#http_handlers).
Les modifications apportées à la section des gestionnaires personnalisés sont détectées lors du rechargement de la config, et le point de terminaison
correspondant redémarre automatiquement.

<div id="some-modules-can-contain-specific-for-its-layer-parameters">
  ### Spécifier des paramètres de couche supplémentaires
</div>

Certains modules peuvent inclure des paramètres de couche supplémentaires. Par exemple, la couche TLS
permet de spécifier une clé privée (`privateKeyFile`) et des fichiers de certificat (`certificateFile`)
comme suit :

```xml
<protocols>

  <plain_http>
    <type>http</type>
    <host>127.0.0.1</host>
    <port>8123</port>
  </plain_http>

  <https>
    <type>tls</type>
    <impl>plain_http</impl>
    <host>127.0.0.1</host>
    <port>8443</port>
    <privateKeyFile>another_server.key</privateKeyFile>
    <certificateFile>another_server.crt</certificateFile>
  </https>

</protocols>
```