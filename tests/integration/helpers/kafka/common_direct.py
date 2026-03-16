import json
import io
import logging
import math
import os.path as p
import random
import socket
import string
import threading

import time
from contextlib import contextmanager

import pytest

import avro.datafile
import avro.io
import avro.schema
from confluent_kafka.avro.cached_schema_registry_client import (
    CachedSchemaRegistryClient,
)

import kafka.errors
from confluent_kafka.avro.serializer.message_serializer import MessageSerializer
from kafka import BrokerConnection, KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
from kafka.protocol.admin import DescribeGroupsRequest_v1
from kafka.protocol.group import MemberAssignment

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster, is_arm
from helpers.network import PartitionManager
from helpers.test_tools import TSV, assert_eq_with_retry

from helpers.kafka import kafka_pb2, social_pb2, message_with_repeated_pb2, oneof_transaction_pb2

from google.protobuf.internal.encoder import _VarintBytes


if is_arm():
    pytestmark = pytest.mark.skip
