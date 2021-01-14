---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 48
toc_title: "Control de acceso y gesti\xF3n de cuentas"
---

# Control de acceso y gestión de cuentas {#access-control}

ClickHouse admite la administración de control de acceso basada en [RBAC](https://en.wikipedia.org/wiki/Role-based_access_control) enfoque.

Entidades de acceso de ClickHouse:
- [Cuenta de usuario](#user-account-management)
- [Rol](#role-management)
- [Política de fila](#row-policy-management)
- [Perfil de configuración](#settings-profiles-management)
- [Cuota](#quotas-management)

Puede configurar entidades de acceso utilizando:

-   Flujo de trabajo controlado por SQL.

    Es necesario [permitir](#enabling-access-control) esta funcionalidad.

-   Servidor [archivos de configuración](configuration-files.md) `users.xml` y `config.xml`.

Se recomienda utilizar el flujo de trabajo controlado por SQL. Ambos métodos de configuración funcionan simultáneamente, por lo que si utiliza los archivos de configuración del servidor para administrar cuentas y derechos de acceso, puede pasar suavemente al flujo de trabajo controlado por SQL.

!!! note "Advertencia"
    No puede administrar la misma entidad de acceso mediante ambos métodos de configuración simultáneamente.

## Uso {#access-control-usage}

De forma predeterminada, el servidor ClickHouse proporciona la cuenta de usuario `default` que no está permitido usar control de acceso controlado por SQL y administración de cuentas, pero tiene todos los derechos y permisos. El `default` cuenta de usuario se utiliza en cualquier caso cuando el nombre de usuario no está definido, por ejemplo, al iniciar sesión desde el cliente o en consultas distribuidas. En el procesamiento de consultas distribuidas se utiliza una cuenta de usuario predeterminada, si la configuración del servidor o clúster no [usuario y contraseña](../engines/table-engines/special/distributed.md) propiedad.

Si acaba de comenzar a usar ClickHouse, puede usar el siguiente escenario:

1.  [Permitir](#enabling-access-control) Control de acceso basado en SQL y gestión de cuentas `default` usuario.
2.  Inicie sesión bajo el `default` cuenta de usuario y crear todos los usuarios. No olvides crear una cuenta de administrador (`GRANT ALL ON *.* WITH GRANT OPTION TO admin_user_account`).
3.  [Restringir permisos](settings/permissions-for-queries.md#permissions_for_queries) para el `default` usuario y deshabilitar el control de acceso impulsado por SQL y la administración de cuentas para ello.

### Propiedades de la solución actual {#access-control-properties}

-   Puede conceder permisos para bases de datos y tablas incluso si no existen.
-   Si se eliminó una tabla, no se revocarán todos los privilegios que corresponden a esta tabla. Por lo tanto, si se crea una nueva tabla más tarde con el mismo nombre, todos los privilegios vuelven a ser reales. Para revocar los privilegios correspondientes a la tabla eliminada, debe realizar, por ejemplo, el `REVOKE ALL PRIVILEGES ON db.table FROM ALL` consulta.
-   No hay ninguna configuración de por vida para los privilegios.

## Cuenta de usuario {#user-account-management}

Una cuenta de usuario es una entidad de acceso que permite autorizar a alguien en ClickHouse. Una cuenta de usuario contiene:

-   Información de identificación.
-   [Privilegio](../sql-reference/statements/grant.md#grant-privileges) que definen un ámbito de consultas que el usuario puede realizar.
-   Hosts desde los que se permite la conexión al servidor ClickHouse.
-   Roles otorgados y predeterminados.
-   Configuración con sus restricciones que se aplican de forma predeterminada en el inicio de sesión del usuario.
-   Perfiles de configuración asignados.

Los privilegios a una cuenta de usuario pueden ser otorgados por el [GRANT](../sql-reference/statements/grant.md) consulta o asignando [rol](#role-management). Para revocar privilegios de un usuario, ClickHouse proporciona el [REVOKE](../sql-reference/statements/revoke.md) consulta. Para listar los privilegios de un usuario, utilice - [SHOW GRANTS](../sql-reference/statements/show.md#show-grants-statement) instrucción.

Consultas de gestión:

-   [CREATE USER](../sql-reference/statements/create.md#create-user-statement)
-   [ALTER USER](../sql-reference/statements/alter.md#alter-user-statement)
-   [DROP USER](../sql-reference/statements/misc.md#drop-user-statement)
-   [SHOW CREATE USER](../sql-reference/statements/show.md#show-create-user-statement)

### Ajustes Aplicación {#access-control-settings-applying}

Los ajustes se pueden establecer de diferentes maneras: para una cuenta de usuario, en sus roles y perfiles de configuración concedidos. En un inicio de sesión de usuario, si se establece una configuración en diferentes entidades de acceso, el valor y las restricciones de esta configuración se aplican mediante las siguientes prioridades (de mayor a menor):

1.  Configuración de la cuenta de usuario.
2.  La configuración de los roles predeterminados de la cuenta de usuario. Si se establece una configuración en algunos roles, el orden de la configuración que se aplica no está definido.
3.  La configuración de los perfiles de configuración asignados a un usuario o a sus roles predeterminados. Si se establece una configuración en algunos perfiles, el orden de aplicación de la configuración no está definido.
4.  Ajustes aplicados a todo el servidor de forma predeterminada o desde el [perfil predeterminado](server-configuration-parameters/settings.md#default-profile).

## Rol {#role-management}

Role es un contenedor para las entidades de acceso que se pueden conceder a una cuenta de usuario.

El rol contiene:

-   [Privilegio](../sql-reference/statements/grant.md#grant-privileges)
-   Configuración y restricciones
-   Lista de funciones concedidas

Consultas de gestión:

-   [CREATE ROLE](../sql-reference/statements/create.md#create-role-statement)
-   [ALTER ROLE](../sql-reference/statements/alter.md#alter-role-statement)
-   [DROP ROLE](../sql-reference/statements/misc.md#drop-role-statement)
-   [SET ROLE](../sql-reference/statements/misc.md#set-role-statement)
-   [SET DEFAULT ROLE](../sql-reference/statements/misc.md#set-default-role-statement)
-   [SHOW CREATE ROLE](../sql-reference/statements/show.md#show-create-role-statement)

Los privilegios a un rol pueden ser otorgados por el [GRANT](../sql-reference/statements/grant.md) consulta. Para revocar privilegios de un rol, ClickHouse proporciona el [REVOKE](../sql-reference/statements/revoke.md) consulta.

## Política de fila {#row-policy-management}

La directiva de filas es un filtro que define qué filas está disponible para un usuario o para un rol. La directiva de filas contiene filtros para una tabla específica y una lista de roles y/o usuarios que deben usar esta directiva de filas.

Consultas de gestión:

-   [CREATE ROW POLICY](../sql-reference/statements/create.md#create-row-policy-statement)
-   [ALTER ROW POLICY](../sql-reference/statements/alter.md#alter-row-policy-statement)
-   [DROP ROW POLICY](../sql-reference/statements/misc.md#drop-row-policy-statement)
-   [SHOW CREATE ROW POLICY](../sql-reference/statements/show.md#show-create-row-policy-statement)

## Perfil de configuración {#settings-profiles-management}

El perfil de configuración es una colección de [configuración](settings/index.md). El perfil de configuración contiene configuraciones y restricciones, y una lista de roles y/o usuarios a los que se aplica esta cuota.

Consultas de gestión:

-   [CREATE SETTINGS PROFILE](../sql-reference/statements/create.md#create-settings-profile-statement)
-   [ALTER SETTINGS PROFILE](../sql-reference/statements/alter.md#alter-settings-profile-statement)
-   [DROP SETTINGS PROFILE](../sql-reference/statements/misc.md#drop-settings-profile-statement)
-   [SHOW CREATE SETTINGS PROFILE](../sql-reference/statements/show.md#show-create-settings-profile-statement)

## Cuota {#quotas-management}

La cuota limita el uso de recursos. Ver [Cuota](quotas.md).

La cuota contiene un conjunto de límites para algunas duraciones y una lista de roles y / o usuarios que deben usar esta cuota.

Consultas de gestión:

-   [CREATE QUOTA](../sql-reference/statements/create.md#create-quota-statement)
-   [ALTER QUOTA](../sql-reference/statements/alter.md#alter-quota-statement)
-   [DROP QUOTA](../sql-reference/statements/misc.md#drop-quota-statement)
-   [SHOW CREATE QUOTA](../sql-reference/statements/show.md#show-create-quota-statement)

## Habilitación del control de acceso basado en SQL y la administración de cuentas {#enabling-access-control}

-   Configure un directorio para el almacenamiento de configuraciones.

    ClickHouse almacena las configuraciones de entidades de acceso en la carpeta [access\_control\_path](server-configuration-parameters/settings.md#access_control_path) parámetro de configuración del servidor.

-   Habilite el control de acceso controlado por SQL y la administración de cuentas para al menos una cuenta de usuario.

    De forma predeterminada, el control de acceso controlado por SQL y la administración de cuentas se activan para todos los usuarios. Debe configurar al menos un usuario en el `users.xml` archivo de configuración y asigne 1 al [access\_management](settings/settings-users.md#access_management-user-setting) configuración.

[Artículo Original](https://clickhouse.tech/docs/en/operations/access_rights/) <!--hide-->
