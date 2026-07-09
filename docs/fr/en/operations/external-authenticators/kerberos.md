---
description: 'Les utilisateurs ClickHouse existants et correctement configurés peuvent être authentifiés
  via le protocole d''authentification Kerberos.'
slug: /operations/external-authenticators/kerberos
title: 'Kerberos'
doc_type: 'reference'
---

import SelfManaged from '@site/docs/_snippets/_self_managed_only_no_roadmap.md';

<div id="kerberos">
  # Kerberos
</div>

<SelfManaged />

Les utilisateurs ClickHouse existants et correctement configurés peuvent être authentifiés via le protocole d’authentification Kerberos.

Actuellement, Kerberos ne peut être utilisé qu’en tant qu’authentificateur externe pour des utilisateurs existants, définis dans `users.xml` ou dans les chemins locaux de contrôle d’accès. Ces utilisateurs peuvent uniquement utiliser des requêtes HTTP et doivent pouvoir s’authentifier à l’aide du mécanisme GSS-SPNEGO.

Pour cette approche, Kerberos doit être configuré sur le système et activé dans la config de ClickHouse.

<div id="enabling-kerberos-in-clickhouse">
  ## Activation de Kerberos dans ClickHouse
</div>

Pour activer Kerberos, incluez la section `kerberos` dans `config.xml`. Cette section peut contenir des paramètres supplémentaires.

<div id="parameters">
  #### Paramètres
</div>

* `principal` - nom canonique du principal de service qui sera obtenu et utilisé lors de l’acceptation des contextes de sécurité.
  * Ce paramètre est facultatif. S’il est omis, le principal par défaut sera utilisé.

* `realm` - realm Kerberos utilisé pour restreindre l’authentification aux seules requêtes dont le realm de l’initiateur correspond.
  * Ce paramètre est facultatif. S’il est omis, aucun filtrage supplémentaire par realm ne sera appliqué.

* `keytab` - chemin vers le fichier keytab du service.
  * Ce paramètre est facultatif. S’il est omis, le chemin vers le fichier keytab du service doit être défini dans la variable d’environnement `KRB5_KTNAME`.

Exemple (à placer dans `config.xml`) :

```xml
<clickhouse>
    <!- ... -->
    <kerberos />
</clickhouse>
```

Avec spécification du principal :

```xml
<clickhouse>
    <!- ... -->
    <kerberos>
        <principal>HTTP/clickhouse.example.com@EXAMPLE.COM</principal>
    </kerberos>
</clickhouse>
```

Avec filtrage par realm :

```xml
<clickhouse>
    <!- ... -->
    <kerberos>
        <realm>EXAMPLE.COM</realm>
    </kerberos>
</clickhouse>
```

:::note
Vous ne pouvez définir qu’une seule section `kerberos`. La présence de plusieurs sections `kerberos` entraîne la désactivation de l’authentification Kerberos dans ClickHouse.
:::

:::note
Les sections `principal` et `realm` ne peuvent pas être spécifiées en même temps. La présence simultanée des sections `principal` et `realm` entraîne la désactivation de l’authentification Kerberos dans ClickHouse.
:::

<div id="kerberos-as-an-external-authenticator-for-existing-users">
  ## Kerberos comme authentificateur externe pour les utilisateurs existants
</div>

Kerberos peut être utilisé comme méthode pour vérifier l’identité des utilisateurs définis localement (utilisateurs définis dans `users.xml` ou dans les chemins locaux de contrôle d’accès). Actuellement, **seules** les requêtes via l’interface HTTP peuvent être *authentifiées via Kerberos* (par le mécanisme GSS-SPNEGO).

Le format du nom de principal Kerberos suit généralement ce modèle :

* *primary/instance@REALM*

La partie */instance* peut apparaître zéro, une ou plusieurs fois. **Pour que l’authentification réussisse, la partie *primary* du nom de principal canonique de l’initiateur doit correspondre au nom d’utilisateur authentifié via Kerberos**.

<div id="enabling-kerberos-in-users-xml">
  ### Activation de Kerberos dans `users.xml`
</div>

Pour activer l’authentification Kerberos pour un utilisateur, indiquez la section `kerberos` au lieu de `password` ou d’autres sections similaires dans la définition de l’utilisateur.

Paramètres :

* `realm` - un realm utilisé pour limiter l’authentification aux seules requêtes dont le realm de l’initiateur correspond.
  * Ce paramètre est facultatif ; s’il est omis, aucun filtrage supplémentaire par realm ne sera appliqué.

Exemple (à placer dans `users.xml`) :

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
Notez que l’authentification Kerberos ne peut pas être utilisée avec un autre mécanisme d’authentification. La présence de toute autre section, telle que `password`, en plus de `kerberos`, entraînera l’arrêt de ClickHouse.
:::

:::info Rappel
Notez que, désormais, dès lors que l’utilisateur `my_user` utilise `kerberos`, Kerberos doit être activé dans le fichier principal `config.xml`, comme décrit précédemment.
:::

<div id="enabling-kerberos-using-sql">
  ### Activation de Kerberos à l’aide de SQL
</div>

Lorsque le [contrôle d’accès et la gestion des comptes pilotés par SQL](/fr/operations/access-rights#access-control-usage) sont activés dans ClickHouse, il est également possible de créer avec des instructions SQL des utilisateurs identifiés par Kerberos.

```sql
CREATE USER my_user IDENTIFIED WITH kerberos REALM 'EXAMPLE.COM'
```

...ou, sans filtrer selon le realm :

```sql
CREATE USER my_user IDENTIFIED WITH kerberos
```