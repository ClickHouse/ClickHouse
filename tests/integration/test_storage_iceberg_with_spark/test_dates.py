import pytest

from helpers.iceberg_utils import (
    create_iceberg_table,
    get_uuid_str,
    default_upload_directory,
    default_download_directory
)


def test_date_reads(started_cluster_iceberg_with_spark):
    instance = started_cluster_iceberg_with_spark.instances["node1"]
    spark = started_cluster_iceberg_with_spark.spark_session
    storage_type = 's3'
    expected_rows=2
    expected_date_1='2299-12-31\n'
    expected_date_2='1900-01-13\n'
    

    TABLE_NAME = (
        "test_date_reads_"
        + storage_type
        + "_"
        + get_uuid_str()
    )

    spark.sql(
        f"""
            CREATE TABLE {TABLE_NAME} (
                number INT,
		date_col DATE	
            )
            USING iceberg
	"""
    )
    spark.sql(
      	  f""" INSERT INTO {TABLE_NAME} VALUES(1,DATE '2299-12-31') """
    ) 
    spark.sql(
      	  f""" INSERT INTO {TABLE_NAME} VALUES(2,DATE '1900-01-13') """
    ) 
   
    files = default_upload_directory(
        started_cluster_iceberg_with_spark,
        storage_type,
        f"/iceberg_data/default/{TABLE_NAME}/",
        f"/iceberg_data/default/{TABLE_NAME}/",
    )


    create_iceberg_table(storage_type, instance, TABLE_NAME, started_cluster_iceberg_with_spark)
    rows_in_ch = int(
      instance.query(
             f"SELECT count() FROM {TABLE_NAME}",
      )
    )
    
    assert rows_in_ch == expected_rows, f"Expected {expected_rows} rows, but got {rows_in_ch}"
    
    ret_date_1 = (
      instance.query(
             f"SELECT date_col FROM {TABLE_NAME} where number=1",
      )
    )
    
    assert ret_date_1==expected_date_1, f"Expected {expected_date_1} rows, but got {ret_date_1}"
    
    ret_date_2 = (
      instance.query(
             f"SELECT date_col FROM {TABLE_NAME} where number=2",
      )
    )
    
    assert ret_date_2==expected_date_2, f"Expected {expected_date_2} rows, but got {ret_date_2}"
