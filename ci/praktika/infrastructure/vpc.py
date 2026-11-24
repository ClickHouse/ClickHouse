from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from ._utils import aws_client


class VPC:

    @dataclass
    class Subnet:
        availability_zone: str
        cidr: str = "10.0.0.0/18"

    @dataclass
    class Lookup:
        """Resolve VPC, subnet, and security-group IDs by Name tag at deploy time.

        Used by infrastructure components that take a `vpc_name` config option
        and need to translate it into VPC/subnet/SG IDs without plumbing the
        `VPC.Config` object through. The VPC id is cached internally so chained
        lookups (subnet + SGs) only `DescribeVpcs` once.
        """

        name: str
        region: str
        _ec2: Any = field(default=None, init=False, repr=False)
        _vpc_id: str = field(default="", init=False, repr=False)

        def _client(self):
            if self._ec2 is None:
                self._ec2 = aws_client("ec2", self.region, f"vpc-lookup-{self.name}")
            return self._ec2

        @property
        def vpc_id(self) -> str:
            if not self._vpc_id:
                resp = self._client().describe_vpcs(
                    Filters=[{"Name": "tag:Name", "Values": [self.name]}]
                )
                vpcs = resp.get("Vpcs", [])
                if not vpcs:
                    raise ValueError(
                        f"VPC '{self.name}' not found in region {self.region}"
                    )
                self._vpc_id = vpcs[0]["VpcId"]
            return self._vpc_id

        def first_subnet_id(self) -> str:
            """Pick a stable subnet from the VPC. Sorted by AZ name so
            re-deploys land in the same one."""
            resp = self._client().describe_subnets(
                Filters=[{"Name": "vpc-id", "Values": [self.vpc_id]}]
            )
            subnets = resp.get("Subnets", [])
            if not subnets:
                raise ValueError(
                    f"VPC '{self.name}' ({self.vpc_id}) has no subnets — "
                    f"deploy the VPC first"
                )
            subnets.sort(key=lambda s: s.get("AvailabilityZone", ""))
            return subnets[0]["SubnetId"]

        def subnet_id_for_az(self, az: str) -> str:
            """Resolve the subnet in a specific availability zone. Sorted by
            SubnetId so re-deploys land in the same one when an AZ has several."""
            resp = self._client().describe_subnets(
                Filters=[
                    {"Name": "vpc-id", "Values": [self.vpc_id]},
                    {"Name": "availability-zone", "Values": [az]},
                ]
            )
            subnets = resp.get("Subnets", [])
            if not subnets:
                raise ValueError(
                    f"VPC '{self.name}' ({self.vpc_id}) has no subnet in "
                    f"availability zone '{az}'"
                )
            subnets.sort(key=lambda s: s.get("SubnetId", ""))
            return subnets[0]["SubnetId"]

        def resolve_security_group_ids(self, names) -> List[str]:
            """Translate SG names to IDs, scoped to this VPC. Empty input → []."""
            names = list(names or [])
            if not names:
                return []
            resp = self._client().describe_security_groups(
                Filters=[
                    {"Name": "group-name", "Values": names},
                    {"Name": "vpc-id", "Values": [self.vpc_id]},
                ]
            )
            by_name = {sg["GroupName"]: sg["GroupId"] for sg in resp.get("SecurityGroups", [])}
            out = []
            for n in names:
                if n not in by_name:
                    raise ValueError(
                        f"Security group '{n}' not found in VPC '{self.name}' ({self.vpc_id})"
                    )
                out.append(by_name[n])
            return out

    @dataclass
    class Config:
        """A self-contained VPC setup: VPC, subnets, internet gateway, and route table.

        Idempotent: all resources are looked up by Name tag before creation.
        Deploy this before ASGs/LaunchTemplates that reference the VPC by name.

        Example::

            vpc = VPC.Config(
                name="ci-cd",
                cidr="10.0.0.0/16",
                region="eu-north-1",
                subnets=[
                    VPC.Subnet(cidr="10.0.0.0/24", availability_zone="eu-north-1a"),
                ],
            )
        """

        name: str = ""
        cidr: str = "10.0.0.0/16"
        region: str = ""
        subnets: List["VPC.Subnet"] = field(default_factory=list)
        ext: Dict[str, Any] = field(default_factory=dict)

        def _tag(self, name: str):
            return [{"Key": "Name", "Value": name}]

        def _find_by_name(self, ec2, resource_type: str, name: str) -> Optional[str]:
            resp = ec2.describe_tags(Filters=[
                {"Name": "resource-type", "Values": [resource_type]},
                {"Name": "tag:Name", "Values": [name]},
            ])
            tags = resp.get("Tags", [])
            return tags[0]["ResourceId"] if tags else None

        def deploy(self):
            ec2 = aws_client("ec2", self.region, self.name)

            # VPC
            vpc_id = self._find_by_name(ec2, "vpc", self.name)
            if vpc_id:
                print(f"VPC '{self.name}' already exists: {vpc_id}")
            else:
                resp = ec2.create_vpc(CidrBlock=self.cidr)
                vpc_id = resp["Vpc"]["VpcId"]
                ec2.create_tags(Resources=[vpc_id], Tags=self._tag(self.name))
                ec2.modify_vpc_attribute(VpcId=vpc_id, EnableDnsHostnames={"Value": True})
                ec2.modify_vpc_attribute(VpcId=vpc_id, EnableDnsSupport={"Value": True})
                print(f"Created VPC '{self.name}': {vpc_id}")
            self.ext["vpc_id"] = vpc_id

            # Internet Gateway
            igw_name = f"{self.name}-igw"
            igw_id = self._find_by_name(ec2, "internet-gateway", igw_name)
            if igw_id:
                print(f"Internet Gateway '{igw_name}' already exists: {igw_id}")
            else:
                igw_id = ec2.create_internet_gateway()["InternetGateway"]["InternetGatewayId"]
                ec2.create_tags(Resources=[igw_id], Tags=self._tag(igw_name))
                print(f"Created Internet Gateway '{igw_name}': {igw_id}")

            # Attach IGW to VPC if not already attached
            igw_info = ec2.describe_internet_gateways(InternetGatewayIds=[igw_id])["InternetGateways"][0]
            attached_vpcs = [a["VpcId"] for a in igw_info.get("Attachments", [])]
            if vpc_id not in attached_vpcs:
                ec2.attach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
                print("Attached Internet Gateway to VPC")
            self.ext["igw_id"] = igw_id

            # Route Table
            rt_name = f"{self.name}-rt"
            rt_id = self._find_by_name(ec2, "route-table", rt_name)
            if rt_id:
                print(f"Route table '{rt_name}' already exists: {rt_id}")
            else:
                rt_id = ec2.create_route_table(VpcId=vpc_id)["RouteTable"]["RouteTableId"]
                ec2.create_tags(Resources=[rt_id], Tags=self._tag(rt_name))
                ec2.create_route(RouteTableId=rt_id, DestinationCidrBlock="0.0.0.0/0", GatewayId=igw_id)
                print(f"Created route table '{rt_name}' with default route: {rt_id}")
            self.ext["route_table_id"] = rt_id

            # Subnets
            subnet_ids = []
            for subnet in self.subnets:
                subnet_name = f"{self.name}-{subnet.availability_zone}"
                subnet_id = self._find_by_name(ec2, "subnet", subnet_name)
                if subnet_id:
                    print(f"Subnet '{subnet_name}' already exists: {subnet_id}")
                else:
                    subnet_id = ec2.create_subnet(
                        VpcId=vpc_id,
                        CidrBlock=subnet.cidr,
                        AvailabilityZone=subnet.availability_zone,
                    )["Subnet"]["SubnetId"]
                    ec2.create_tags(Resources=[subnet_id], Tags=self._tag(subnet_name))
                    ec2.modify_subnet_attribute(SubnetId=subnet_id, MapPublicIpOnLaunch={"Value": True})
                    ec2.associate_route_table(RouteTableId=rt_id, SubnetId=subnet_id)
                    print(f"Created subnet '{subnet_name}' ({subnet.cidr}): {subnet_id}")
                subnet_ids.append(subnet_id)
            self.ext["subnet_ids"] = subnet_ids

            # Default security group — allows all outbound, blocks all inbound
            sg_name = f"{self.name}-sg"
            existing_sgs = ec2.describe_security_groups(Filters=[
                {"Name": "vpc-id", "Values": [vpc_id]},
                {"Name": "group-name", "Values": [sg_name]},
            ])["SecurityGroups"]
            if existing_sgs:
                sg_id = existing_sgs[0]["GroupId"]
                print(f"Security group '{sg_name}' already exists: {sg_id}")
            else:
                sg_id = ec2.create_security_group(
                    GroupName=sg_name,
                    Description=f"Default security group for {self.name}",
                    VpcId=vpc_id,
                )["GroupId"]
                ec2.create_tags(Resources=[sg_id], Tags=self._tag(sg_name))
                print(f"Created security group '{sg_name}': {sg_id}")
            self.ext["default_sg_id"] = sg_id
            self.ext["default_sg_name"] = sg_name

            print(f"VPC '{self.name}' ready (vpc_id={vpc_id}, sg={sg_id})")
            return self

        def delete(self):
            ec2 = aws_client("ec2", self.region, self.name)

            vpc_id = self._find_by_name(ec2, "vpc", self.name)
            if not vpc_id:
                print(f"VPC '{self.name}' does not exist, skipping")
                return

            # Delete subnets
            subnets = ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])["Subnets"]
            for s in subnets:
                ec2.delete_subnet(SubnetId=s["SubnetId"])
                print(f"Deleted subnet {s['SubnetId']}")

            # Delete route tables (non-main)
            rts = ec2.describe_route_tables(Filters=[{"Name": "vpc-id", "Values": [vpc_id]}])["RouteTables"]
            for rt in rts:
                if not any(a.get("Main") for a in rt.get("Associations", [])):
                    ec2.delete_route_table(RouteTableId=rt["RouteTableId"])
                    print(f"Deleted route table {rt['RouteTableId']}")

            # Detach and delete internet gateway
            igws = ec2.describe_internet_gateways(Filters=[{"Name": "attachment.vpc-id", "Values": [vpc_id]}])["InternetGateways"]
            for igw in igws:
                igw_id = igw["InternetGatewayId"]
                ec2.detach_internet_gateway(InternetGatewayId=igw_id, VpcId=vpc_id)
                ec2.delete_internet_gateway(InternetGatewayId=igw_id)
                print(f"Deleted internet gateway {igw_id}")

            # Delete non-default security groups
            sgs = ec2.describe_security_groups(
                Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
            )["SecurityGroups"]
            for sg in sgs:
                if sg["GroupName"] != "default":
                    try:
                        ec2.delete_security_group(GroupId=sg["GroupId"])
                        print(f"Deleted security group {sg['GroupId']} ({sg['GroupName']})")
                    except Exception as e:
                        print(f"Warning: Could not delete SG {sg['GroupId']}: {e}")

            ec2.delete_vpc(VpcId=vpc_id)
            print(f"Deleted VPC '{self.name}' ({vpc_id})")
