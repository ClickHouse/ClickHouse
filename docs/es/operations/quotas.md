# Cuota {#quotas}

Las cuotas le permiten limitar el uso de recursos durante un período de tiempo, o simplemente realizar un seguimiento del uso de recursos.
Las cuotas se configuran en la configuración del usuario. Esto suele ser ‘users.xml’.

El sistema también tiene una característica para limitar la complejidad de una sola consulta. Vea la sección “Restrictions on query complexity”).

A diferencia de las restricciones de complejidad de consultas, las cuotas:

-   Coloque restricciones en un conjunto de consultas que se pueden ejecutar durante un período de tiempo, en lugar de limitar una sola consulta.
-   Tenga en cuenta los recursos gastados en todos los servidores remotos para el procesamiento de consultas distribuidas.

Echemos un vistazo a la sección de la ‘users.xml’ fichero que define las cuotas.

``` xml
<!-- Quotas -->
<quotas>
    <!-- Quota name. -->
    <default>
        <!-- Restrictions for a time period. You can set many intervals with different restrictions. -->
        <interval>
            <!-- Length of the interval. -->
            <duration>3600</duration>

            <!-- Unlimited. Just collect data for the specified time interval. -->
            <queries>0</queries>
            <errors>0</errors>
            <result_rows>0</result_rows>
            <read_rows>0</read_rows>
            <execution_time>0</execution_time>
        </interval>
    </default>
```

De forma predeterminada, la cuota solo realiza un seguimiento del consumo de recursos para cada hora, sin limitar el uso.
El consumo de recursos calculado para cada intervalo se envía al registro del servidor después de cada solicitud.

``` xml
<statbox>
    <!-- Restrictions for a time period. You can set many intervals with different restrictions. -->
    <interval>
        <!-- Length of the interval. -->
        <duration>3600</duration>

        <queries>1000</queries>
        <errors>100</errors>
        <result_rows>1000000000</result_rows>
        <read_rows>100000000000</read_rows>
        <execution_time>900</execution_time>
    </interval>

    <interval>
        <duration>86400</duration>

        <queries>10000</queries>
        <errors>1000</errors>
        <result_rows>5000000000</result_rows>
        <read_rows>500000000000</read_rows>
        <execution_time>7200</execution_time>
    </interval>
</statbox>
```

Para el ‘statbox’ Las restricciones se establecen por cada hora y por cada 24 horas (86.400 segundos). El intervalo de tiempo se cuenta a partir de un momento fijo definido por la implementación en el tiempo. En otras palabras, el intervalo de 24 horas no necesariamente comienza a medianoche.

Cuando finaliza el intervalo, se borran todos los valores recopilados. Para la siguiente hora, el cálculo de la cuota comienza de nuevo.

Estas son las cantidades que se pueden restringir:

`queries` – El número total de solicitudes.

`errors` – El número de consultas que lanzaron una excepción.

`result_rows` – El número total de filas dadas como resultado.

`read_rows` – El número total de filas de origen leídas de las tablas para ejecutar la consulta, en todos los servidores remotos.

`execution_time` – El tiempo total de ejecución de la consulta, en segundos (tiempo de pared).

Si se excede el límite durante al menos un intervalo de tiempo, se lanza una excepción con un texto sobre qué restricción se excedió, para qué intervalo y cuándo comienza el nuevo intervalo (cuando se pueden enviar consultas nuevamente).

Las cuotas pueden usar el “quota key” característica con el fin de informar sobre los recursos para múltiples claves de forma independiente. Aquí hay un ejemplo de esto:

``` xml
<!-- For the global reports designer. -->
<web_global>
    <!-- keyed – The quota_key "key" is passed in the query parameter,
            and the quota is tracked separately for each key value.
        For example, you can pass a Yandex.Metrica username as the key,
            so the quota will be counted separately for each username.
        Using keys makes sense only if quota_key is transmitted by the program, not by a user.

        You can also write <keyed_by_ip /> so the IP address is used as the quota key.
        (But keep in mind that users can change the IPv6 address fairly easily.)
    -->
    <keyed />
```

La cuota se asigna a los usuarios ‘users’ sección de la configuración. Vea la sección “Access rights”.

Para el procesamiento de consultas distribuidas, los importes acumulados se almacenan en el servidor del solicitante. Entonces, si el usuario va a otro servidor, la cuota allí “start over”.

Cuando se reinicia el servidor, las cuotas se restablecen.

[Artículo Original](https://clickhouse.tech/docs/es/operations/quotas/) <!--hide-->
