---
description: 'Paramètres de configuration des utilisateurs et des rôles.'
sidebar_label: 'Paramètres utilisateur'
sidebar_position: 63
slug: /operations/settings/settings-users
title: 'Paramètres des utilisateurs et des rôles'
doc_type: 'reference'
---

La section `users` du fichier de configuration `users.xml` contient les paramètres utilisateur.

:::note
ClickHouse prend également en charge le [workflow piloté par SQL](/fr/operations/access-rights#access-control-usage) pour la gestion des utilisateurs. Nous recommandons de l&#39;utiliser.
:::

Structure de la section `users` :

```xml
<users>
    <!-- If user name was not specified, 'default' user is used. -->
    <user_name>
        <!-- Exactly one authentication method may be specified at the users.user_name level. For example: -->
        <password></password>
        <!-- Or (exclusive) -->
        <password_sha256_hex></password_sha256_hex>
 
        <!-- Or (exclusive) (N.B. multiple SSH keys are allowed for backwards compatibility) -->
        <ssh_keys>
            <ssh_key>
                <type>ssh-ed25519</type>
                <base64_key>AAAAC3NzaC1lZDI1NTE5AAAAIDNf0r6vRl24Ix3tv2IgPmNPO2ATa2krvt80DdcTatLj</base64_key>
            </ssh_key>
            <ssh_key>
                <type>ecdsa-sha2-nistp256</type>
                <base64_key>AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBNxeV2uN5UY6CUbCzTA1rXfYimKQA5ivNIqxdax4bcMXz4D0nSk2l5E1TkR5mG8EBWtmExSPbcEPJ8V7lyWWbA8=</base64_key>
            </ssh_key>
            <ssh_key>
                <type>ssh-rsa</type>
                <base64_key>AAAAB3NzaC1yc2EAAAADAQABAAABgQCpgqL1SHhPVBOTFlOm0pu+cYBbADzC2jL41sPMawYCJHDyHuq7t+htaVVh2fRgpAPmSEnLEC2d4BEIKMtPK3bfR8plJqVXlLt6Q8t4b1oUlnjb3VPA9P6iGcW7CV1FBkZQEVx8ckOfJ3F+kI5VsrRlEDgiecm/C1VPl0/9M2llW/mPUMaD65cM9nlZgM/hUeBrfxOEqM11gDYxEZm1aRSbZoY4dfdm3vzvpSQ6lrCrkjn3X2aSmaCLcOWJhfBWMovNDB8uiPuw54g3ioZ++qEQMlfxVsqXDGYhXCrsArOVuW/5RbReO79BvXqdssiYShfwo+GhQ0+aLWMIW/jgBkkqx/n7uKLzCMX7b2F+aebRYFh+/QXEj7SnihdVfr9ud6NN3MWzZ1ltfIczlEcFLrLJ1Yq57wW6wXtviWh59WvTWFiPejGjeSjjJyqqB49tKdFVFuBnIU5u/bch2DXVgiAEdQwUrIp1ACoYPq22HFFAYUJrL32y7RxX3PGzuAv3LOc=</base64_key>
            </ssh_key>
        </ssh_keys>

        <!-- Or (exclusive) for multiple authentication methods: -->
        <auth_methods>
            <method1>
                <password></password>
            </method1>
            <method2>
                <password_sha256_hex></password_sha256_hex>
            </method2>
            <!-- ... -->
            <methodN>
                <!-- ... -->
            </methodN>
        </auth_methods>

        <access_management>0|1</access_management>

        <networks incl="networks" replace="replace">
        </networks>

        <profile>profile_name</profile>

        <quota>default</quota>
        <default_database>default</default_database>
        <databases>
            <database_name>
                <table_name>
                    <filter>expression</filter>
                </table_name>
            </database_name>
        </databases>

        <grants>
            <query>GRANT SELECT ON system.*</query>
        </grants>
    </user_name>
    <!-- Other users settings -->
</users>
```

<div id="user-namepassword">
  ### user_name/password
</div>

Le mot de passe peut être spécifié en clair ou en SHA256 (format hexadécimal).

* Pour définir un mot de passe en clair (**non recommandé**), placez-le dans un élément `password`.

  Par exemple, `<password>qwerty</password>`. Le mot de passe peut être laissé vide.

<a id="password_sha256_hex" />

* Pour définir un mot de passe à l’aide de son hash SHA256, placez-le dans un élément `password_sha256_hex`.

  Par exemple, `<password_sha256_hex>65e84be33532fb784c48129675f9eff3a682b27168c0ea744b2cf58ee02337c5</password_sha256_hex>`.

  Exemple de génération d’un mot de passe depuis le shell :

  ```bash
  PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha256sum | tr -d '-'
  ```

  La première ligne du résultat contient le mot de passe. La deuxième ligne contient le hash SHA256 correspondant.

<a id="password_double_sha1_hex" />

* Pour assurer la compatibilité avec les clients MySQL, le mot de passe peut être spécifié sous forme de hash double SHA1. Placez-le dans l’élément `password_double_sha1_hex`.

  Par exemple, `<password_double_sha1_hex>08b4a0f1de6ad37da17359e592c8d74788a83eb0</password_double_sha1_hex>`.

  Exemple de génération d’un mot de passe depuis le shell :

  ```bash
  PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha1sum | tr -d '-' | xxd -r -p | sha1sum | tr -d '-'
  ```

  La première ligne du résultat contient le mot de passe. La deuxième ligne contient le hash double SHA1 correspondant.

<div id="totp-authentication-configuration">
  ### Configuration de l’authentification TOTP
</div>

Le mot de passe à usage unique basé sur le temps (TOTP) peut être utilisé pour authentifier les utilisateurs ClickHouse en générant des codes d’accès temporaires valables pendant une durée limitée.
Cette méthode d’authentification TOTP est conforme à la norme [RFC 6238](https://datatracker.ietf.org/doc/html/rfc6238), ce qui la rend compatible avec des applications TOTP courantes comme Google Authenticator, 1Password et d’autres outils similaires.
Elle peut être configurée via le fichier de configuration `users.xml`, en complément d’une authentification par mot de passe.
Elle n’est pas encore prise en charge dans le contrôle d’accès piloté par SQL.

Pour s’authentifier avec TOTP, les utilisateurs doivent fournir un mot de passe principal ainsi qu’un mot de passe à usage unique généré par leur application TOTP, soit via l’option de ligne de commande `--one-time-password`, soit en le concaténant au mot de passe principal avec le caractère &#39;+&#39;.
Par exemple, si le mot de passe principal est `some_password` et que le code TOTP généré est `345123`, l’utilisateur peut spécifier `--password some_password+345123` ou `--password some_password --one-time-password 345123` lors de la connexion à ClickHouse. Si aucun mot de passe n’est spécifié, `clickhouse-client` le demandera de façon interactive.

Pour activer l’authentification TOTP pour un utilisateur, configurez la section `time_based_one_time_password` dans `users.xml`. Cette section définit les paramètres TOTP, tels que le secret, la période de validité, le nombre de chiffres et l’algorithme de hachage.

**Exemple**

````xml
<clickhouse>
    <!-- ... -->
    <users>
        <my_user>
            <!-- Primary password-based authentication: -->
            <password>some_password</password>
            <password_sha256_hex>1464acd6765f91fccd3f5bf4f14ebb7ca69f53af91b0a5790c2bba9d8819417b</password_sha256_hex>
            <!-- ... or any other supported authentication method ... -->

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
````

Cette commande produira un secret encodé en base32 qui peut être ajouté au champ `secret` de users.xml.

Pour activer TOTP pour un utilisateur spécifique, ajoutez à n’importe quel champ existant basé sur un mot de passe (comme `password` ou `password_sha256_hex`) une section `time_based_one_time_password` supplémentaire.

L’outil [qrencode](https://linux.die.net/man/1/qrencode) peut être utilisé pour générer un code QR pour le secret TOTP.

```bash
$ qrencode -t ansiutf8 'otpauth://totp/ClickHouse?issuer=ClickHouse&secret=JBSWY3DPEHPK3PXP'
```

Après avoir configuré TOTP pour un utilisateur, un mot de passe à usage unique peut être utilisé dans le processus d’authentification, comme décrit ci-dessus.

### nom d&#39;utilisateur/clé SSH

Ce paramètre permet l&#39;authentification à l&#39;aide de clés SSH.

Étant donnée une clé SSH (générée par `ssh-keygen`) telle que

```text
ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIDNf0r6vRl24Ix3tv2IgPmNPO2ATa2krvt80DdcTatLj john@example.com
```

L’élément `ssh_key` doit être

```xml
<ssh_key>
     <type>ssh-ed25519</type>
     <base64_key>AAAAC3NzaC1lZDI1NTE5AAAAIDNf0r6vRl24Ix3tv2IgPmNPO2ATa2krvt80DdcTatLj</base64_key>
 </ssh_key>
```

Remplacez `ssh-ed25519` par `ssh-rsa` ou `ecdsa-sha2-nistp256` si vous utilisez un autre algorithme pris en charge.

### Méthodes d’authentification multiples

Un même utilisateur peut être configuré avec plusieurs méthodes d’authentification à l’aide de l’élément `<auth_methods>`. Cela permet à l’utilisateur de s’authentifier avec n’importe laquelle des méthodes répertoriées. Par exemple, un utilisateur peut avoir à la fois un mot de passe et des identifiants LDAP, et la connexion avec l’un ou l’autre aboutira.

Chaque élément enfant de `<auth_methods>` est un wrapper nommé arbitrairement qui contient exactement un type d’authentification. Le nom du wrapper (par ex. `<method1>`, `<primary>`, `<a1>`) n’a pas d’importance ; seul l’élément d’authentification interne est pris en compte.

**Exemple : plusieurs mots de passe**

```xml
<users>
    <my_user>
        <auth_methods>
            <primary>
                <password>password_one</password>
            </primary>
            <secondary>
                <password_sha256_hex>65e84be33532fb784c48129675f9eff3a682b27168c0ea744b2cf58ee02337c5</password_sha256_hex>
            </secondary>
        </auth_methods>
    </my_user>
</users>
```

**Exemple : plusieurs types d’authentification**

```xml
<users>
    <my_user>
        <auth_methods>
            <a1>
                <password>plaintext_pass</password>
            </a1>
            <a2>
                <password_sha256_hex>e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855</password_sha256_hex>
            </a2>
            <a3>
                <ldap>
                    <server>my_ldap_server</server>
                </ldap>
            </a3>
        </auth_methods>
    </my_user>
</users>
```

Les types d&#39;authentification suivants sont pris en charge dans `<auth_methods>` :

* **`password`** — mot de passe en clair
* **`password_sha256_hex`** — hachage de mot de passe SHA256
* **`password_scram_sha256_hex`** — hachage de mot de passe SCRAM-SHA-256
* **`password_double_sha1_hex`** — hachage de mot de passe double SHA1
* **`ldap`** — authentification via le serveur LDAP
* **`kerberos`** — authentification Kerberos
* **`ssl_certificates`** — authentification par certificat SSL
* **`ssh_keys`** — authentification par clé SSH
* **`http_authentication`** — authentification HTTP

**Règles et restrictions :**

* `<auth_methods>` **ne peut pas** être utilisé avec des méthodes d&#39;authentification spécifiées au niveau de l&#39;utilisateur. Utilisez soit l&#39;un, soit l&#39;autre, mais pas les deux.
* `<auth_methods>` doit contenir au moins une méthode d&#39;authentification.
* Chaque élément conteneur dans `<auth_methods>` doit contenir exactement un type d&#39;authentification (à l&#39;exception de `<ssh_keys>`, qui peut en contenir plusieurs, pour des raisons de compatibilité descendante).
* TOTP (`<time_based_one_time_password>`) est spécifié au niveau de l&#39;utilisateur (en dehors de `<auth_methods>`) et s&#39;applique à toutes les méthodes basées sur un mot de passe de la liste. Au moins une méthode basée sur un mot de passe est requise lorsque TOTP est activé.

**Exemple : `auth_methods` avec TOTP**

```xml
<users>
    <my_user>
        <auth_methods>
            <a1>
                <password>my_password</password>
            </a1>
            <a2>
                <ldap>
                    <server>ldap_server_1</server>
                </ldap>
            </a2>
        </auth_methods>
        <time_based_one_time_password>
            <secret>JBSWY3DPEHPK3PXP</secret>
        </time_based_one_time_password>
    </my_user>
</users>
```

Dans cet exemple, la vérification TOTP s’applique à la méthode utilisant un mot de passe (`<password>`), tandis que la méthode LDAP s’authentifie indépendamment auprès du serveur externe.

### access_management

Ce paramètre active ou désactive l&#39;utilisation du [contrôle d&#39;accès et de la gestion des comptes](/fr/operations/access-rights#access-control-usage) piloté par SQL pour l&#39;utilisateur.

Valeurs possibles :

* 0 — Désactivé.
* 1 — Activé.

Valeur par défaut : 0.

### grants

Ce paramètre permet d’accorder tous les droits à l’utilisateur sélectionné.
Chaque élément de la liste doit être une requête `GRANT` sans bénéficiaire spécifié.

Exemple :

```xml
<user1>
    <grants>
        <query>GRANT SHOW ON *.*</query>
        <query>GRANT CREATE ON *.* WITH GRANT OPTION</query>
        <query>GRANT SELECT ON system.*</query>
    </grants>
</user1>
```

Ce paramètre ne peut pas être défini en même temps que les paramètres
`dictionaries`, `access_management`, `named_collection_control`, `show_named_collections_secrets`
et `allow_databases`.

### user_name/networks

Liste des réseaux depuis lesquels l’utilisateur peut se connecter au serveur ClickHouse.

Chaque élément de la liste peut prendre l’une des formes suivantes :

* `<ip>` — Adresse IP ou masque réseau.

  Exemples : `213.180.204.3`, `10.0.0.1/8`, `10.0.0.1/255.255.255.0`, `2a02:6b8::3`, `2a02:6b8::3/64`, `2a02:6b8::3/ffff:ffff:ffff:ffff::`.

* `<host>` — Nom d’hôte.

  Exemple : `example01.host.ru`.

  Pour vérifier l’accès, une requête DNS est effectuée et toutes les adresses IP renvoyées sont comparées à l’adresse distante.

* `<host_regexp>` — Expression régulière pour les noms d’hôte.

  Exemple : `^example\d\d-\d\d-\d\.host\.ru$`

  Pour vérifier l’accès, une [requête DNS PTR](https://en.wikipedia.org/wiki/Reverse_DNS_lookup) est effectuée pour l’adresse distante, puis l’expression régulière spécifiée est appliquée. Ensuite, une autre requête DNS est effectuée sur les résultats de la requête PTR et toutes les adresses reçues sont comparées à l’adresse distante. Nous recommandons vivement que l’expression régulière se termine par $.

Tous les résultats des requêtes DNS sont mis en cache jusqu’au redémarrage du serveur.

**Exemples**

Pour ouvrir l’accès à l’utilisateur depuis n’importe quel réseau, spécifiez :

```xml
<ip>::/0</ip>
```

:::note
Il est dangereux d’ouvrir l’accès depuis n’importe quel réseau, sauf si vous avez configuré correctement un pare-feu ou si le serveur n’est pas directement connecté à Internet.
:::

Pour n’ouvrir l’accès que depuis localhost, indiquez :

```xml
<ip>::1</ip>
<ip>127.0.0.1</ip>
```

### user_name/profile

Vous pouvez affecter un profil de paramètres à l’utilisateur. Les profils de paramètres sont configurés dans une section distincte du fichier `users.xml`. Pour plus d’informations, consultez [Profils de paramètres](../../operations/settings/settings-profiles.md).

### user_name/quota

Les quotas vous permettent de suivre ou de limiter l’utilisation des ressources sur une période donnée. Les quotas sont configurés dans la section `quotas`
du fichier de configuration `users.xml`.

Vous pouvez attribuer un jeu de quotas à l’utilisateur. Pour une description détaillée de la configuration des quotas, consultez [Quotas](/fr/operations/quotas).

### user_name/databases

Dans cette section, vous pouvez limiter les lignes renvoyées par ClickHouse pour les requêtes `SELECT` effectuées par l’utilisateur courant, afin de mettre en place une sécurité de base au niveau des lignes.

**Exemple**

La configuration suivante impose que l’utilisateur `user1` ne puisse voir, dans les résultats des requêtes `SELECT`, que les lignes de `table1` pour lesquelles la valeur du champ `id` est 1000.

```xml
<user1>
    <databases>
        <database_name>
            <table1>
                <filter>id = 1000</filter>
            </table1>
        </database_name>
    </databases>
</user1>
```

Le `filter` peut être n’importe quelle expression qui produit une valeur de type [UInt8](../../sql-reference/data-types/int-uint.md). Il contient généralement des comparaisons et des opérateurs logiques. Les lignes de `database_name.table1` pour lesquelles le `filter` renvoie 0 ne sont pas retournées pour cet utilisateur. Le filtrage est incompatible avec les opérations `PREWHERE` et désactive l’optimisation `WHERE→PREWHERE`.

## Rôles

Vous pouvez créer n’importe quel rôle prédéfini à l’aide de la section `roles` du fichier de configuration `user.xml`.

Structure de la section `roles` :

```xml
<roles>
    <test_role>
        <grants>
            <query>GRANT SHOW ON *.*</query>
            <query>REVOKE SHOW ON system.*</query>
            <query>GRANT CREATE ON *.* WITH GRANT OPTION</query>
        </grants>
    </test_role>
</roles>
```

Ces rôles peuvent également être attribués aux utilisateurs dans la section `users` :

```xml
<users>
    <user_name>
        ...
        <grants>
            <query>GRANT test_role</query>
        </grants>
    </user_name>
<users>
```