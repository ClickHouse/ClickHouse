---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 63
toc_title: "Les Param\xE8tres De L'Utilisateur"
---

# Les Paramètres De L'Utilisateur {#user-settings}

Le `users` la section de la `user.xml` le fichier de configuration contient les paramètres utilisateur.

!!! note "Information"
    Clickhouse prend également en charge [Flux de travail piloté par SQL](../access-rights.md#access-control) pour gérer les utilisateurs. Nous vous conseillons de l'utiliser.

La Structure de la `users` section:

``` xml
<users>
    <!-- If user name was not specified, 'default' user is used. -->
    <user_name>
        <password></password>
        <!-- Or -->
        <password_sha256_hex></password_sha256_hex>

        <access_management>0|1</access_management>

        <networks incl="networks" replace="replace">
        </networks>

        <profile>profile_name</profile>

        <quota>default</quota>

        <databases>
            <database_name>
                <table_name>
                    <filter>expression</filter>
                <table_name>
            </database_name>
        </databases>
    </user_name>
    <!-- Other users settings -->
</users>
```

### nom\_utilisateur/mot de passe {#user-namepassword}

Le mot de passe peut être spécifié en texte clair ou en SHA256 (format hexadécimal).

-   Pour attribuer un mot de passe en clair (**pas recommandé**), la placer dans un `password` élément.

    Exemple, `<password>qwerty</password>`. Le mot de passe peut être laissé en blanc.

<a id="password_sha256_hex"></a>

-   Pour attribuer un mot de passe à l'aide de son hachage SHA256, placez-le dans un `password_sha256_hex` élément.

    Exemple, `<password_sha256_hex>65e84be33532fb784c48129675f9eff3a682b27168c0ea744b2cf58ee02337c5</password_sha256_hex>`.

    Exemple de génération d'un mot de passe à partir du shell:

          PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha256sum | tr -d '-'

    La première ligne du résultat est le mot de passe. La deuxième ligne est le hachage SHA256 correspondant.

<a id="password_double_sha1_hex"></a>

-   Pour la compatibilité avec les clients MySQL, le mot de passe peut être spécifié dans le hachage double SHA1. Le placer dans `password_double_sha1_hex` élément.

    Exemple, `<password_double_sha1_hex>08b4a0f1de6ad37da17359e592c8d74788a83eb0</password_double_sha1_hex>`.

    Exemple de génération d'un mot de passe à partir du shell:

          PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha1sum | tr -d '-' | xxd -r -p | sha1sum | tr -d '-'

    La première ligne du résultat est le mot de passe. La deuxième ligne est le double hachage SHA1 correspondant.

### access\_management {#access_management-user-setting}

Ce paramètre active de désactive l'utilisation de SQL-driven [le contrôle d'accès et de gestion de compte](../access-rights.md#access-control) pour l'utilisateur.

Valeurs possibles:

-   0 — Disabled.
-   1 — Enabled.

Valeur par défaut: 0.

### nom\_utilisateur / réseaux {#user-namenetworks}

Liste des réseaux à partir desquels L'utilisateur peut se connecter au serveur ClickHouse.

Chaque élément de la liste peut avoir l'une des formes suivantes:

-   `<ip>` — IP address or network mask.

    Exemple: `213.180.204.3`, `10.0.0.1/8`, `10.0.0.1/255.255.255.0`, `2a02:6b8::3`, `2a02:6b8::3/64`, `2a02:6b8::3/ffff:ffff:ffff:ffff::`.

-   `<host>` — Hostname.

    Exemple: `example01.host.ru`.

    Pour vérifier l'accès, une requête DNS est effectuée et toutes les adresses IP renvoyées sont comparées à l'adresse homologue.

-   `<host_regexp>` — Regular expression for hostnames.

    Exemple, `^example\d\d-\d\d-\d\.host\.ru$`

    Pour vérifier l'accès, un [Requête DNS PTR](https://en.wikipedia.org/wiki/Reverse_DNS_lookup) est effectuée pour l'adresse homologue, puis l'expression rationnelle spécifiée est appliquée. Ensuite, une autre requête DNS est effectuée pour les résultats de la requête PTR et toutes les adresses reçues sont comparées à l'adresse homologue. Nous recommandons fortement que regexp se termine avec $.

Tous les résultats des requêtes DNS sont mis en cache jusqu'au redémarrage du serveur.

**Exemple**

Pour ouvrir l'accès de l'utilisateur à partir de n'importe quel réseau, spécifiez:

``` xml
<ip>::/0</ip>
```

!!! warning "Avertissement"
    Il n'est pas sûr d'ouvrir l'accès à partir de n'importe quel réseau, sauf si vous avez un pare-feu correctement configuré ou si le serveur n'est pas directement connecté à Internet.

Pour ouvrir l'accès uniquement à partir de localhost, spécifier:

``` xml
<ip>::1</ip>
<ip>127.0.0.1</ip>
```

### nom\_utilisateur / profil {#user-nameprofile}

Vous pouvez attribuer un profil des paramètres pour l'utilisateur. Les profils de paramètres sont configurés dans une section distincte du `users.xml` fichier. Pour plus d'informations, voir [Profils des paramètres](settings-profiles.md).

### nom\_utilisateur / quota {#user-namequota}

Les Quotas vous permettent de suivre ou de limiter l'utilisation des ressources sur une période donnée. Les Quotas sont configurés dans le `quotas`
la section de la `users.xml` fichier de configuration.

Vous pouvez attribuer un jeu de quotas à l'utilisateur. Pour une description détaillée de la configuration des quotas, voir [Quota](../quotas.md#quotas).

### nom\_utilisateur/bases de données {#user-namedatabases}

Dans cette section, vous pouvez limiter les lignes renvoyées par ClickHouse pour `SELECT` requêtes faites par l'utilisateur actuel, implémentant ainsi la sécurité de base au niveau de la ligne.

**Exemple**

La configuration suivante force cet utilisateur `user1` ne peut voir les lignes de `table1` comme le résultat de `SELECT` requêtes, où la valeur de la `id` le champ est 1000.

``` xml
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

Le `filter` peut être n'importe quelle expression résultant en un [UInt8](../../sql-reference/data-types/int-uint.md)-le type de la valeur. Il contient généralement des comparaisons et des opérateurs logiques. Les lignes de `database_name.table1` où filtrer les résultats à 0 ne sont pas retournés pour cet utilisateur. Le filtrage est incompatible avec `PREWHERE` opérations et désactive `WHERE→PREWHERE` optimisation.

[Article Original](https://clickhouse.tech/docs/en/operations/settings/settings_users/) <!--hide-->
