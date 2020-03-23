# Restricciones en la configuración {#constraints-on-settings}

Las restricciones en los ajustes se pueden definir en el `profiles` sección de la `user.xml` el archivo de configuración y prohíba a los usuarios cambiar algunos de los ajustes `SET` consulta.
Las restricciones se definen como las siguientes:

``` xml
<profiles>
  <user_name>
    <constraints>
      <setting_name_1>
        <min>lower_boundary</min>
      </setting_name_1>
      <setting_name_2>
        <max>upper_boundary</max>
      </setting_name_2>
      <setting_name_3>
        <min>lower_boundary</min>
        <max>upper_boundary</max>
      </setting_name_3>
      <setting_name_4>
        <readonly/>
      </setting_name_4>
    </constraints>
  </user_name>
</profiles>
```

Si el usuario intenta violar las restricciones, se produce una excepción y no se cambia la configuración.
Se admiten tres tipos de restricciones: `min`, `max`, `readonly`. El `min` y `max` Las restricciones especifican los límites superior e inferior para una configuración numérica y se pueden usar en combinación. El `readonly` constraint especifica que el usuario no puede cambiar la configuración correspondiente en absoluto.

**Ejemplo:** Dejar `users.xml` incluye líneas:

``` xml
<profiles>
  <default>
    <max_memory_usage>10000000000</max_memory_usage>
    <force_index_by_date>0</force_index_by_date>
    ...
    <constraints>
      <max_memory_usage>
        <min>5000000000</min>
        <max>20000000000</max>
      </max_memory_usage>
      <force_index_by_date>
        <readonly/>
      </force_index_by_date>
    </constraints>
  </default>
</profiles>
```

Las siguientes consultas arrojan excepciones:

``` sql
SET max_memory_usage=20000000001;
SET max_memory_usage=4999999999;
SET force_index_by_date=1;
```

``` text
Code: 452, e.displayText() = DB::Exception: Setting max_memory_usage should not be greater than 20000000000.
Code: 452, e.displayText() = DB::Exception: Setting max_memory_usage should not be less than 5000000000.
Code: 452, e.displayText() = DB::Exception: Setting force_index_by_date should not be changed.
```

**Nota:** el `default` perfil tiene un manejo especial: todas las restricciones definidas para el `default` perfil se convierten en las restricciones predeterminadas, por lo que restringen todos los usuarios hasta que se anulan explícitamente para estos usuarios.

[Artículo Original](https://clickhouse.tech/docs/es/operations/settings/constraints_on_settings/) <!--hide-->
