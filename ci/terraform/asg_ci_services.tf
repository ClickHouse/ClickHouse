# Launch Template with Ubuntu AMI
resource "aws_launch_template" "ci-services-template" {
  name_prefix   = "ci-services-template"
  image_id      = "ami-0c14ff330901e49ff"  # Ubuntu 24.04 LTS, arm, AMI us-east-1
  instance_type = "t4g.small"

  key_name = var.key_name

  lifecycle {
    ignore_changes = [
      # image_id,
      # description,
      # tags["CreatedBy"],
      # tags_all["CreatedBy"],
    ]
  }

  # User data to install a package upon launch
  user_data = base64encode(data.local_file.user_data_ci_services.content)

  iam_instance_profile {
    name = "ec2_admin"
  }

  block_device_mappings {
    device_name = "/dev/sda1"
    ebs {
      volume_size = 8
      volume_type = "gp3"
    }
  }

  vpc_security_group_ids = [var.sg_id]

  tag_specifications {
    resource_type = "instance"

    tags = {
      "github:runner-type" = var.runner_ci_services
    }
  }
  tag_specifications {
    resource_type = "volume"

    tags = {
      "github:runner-type" = var.runner_ci_services
    }
  }
  tag_specifications {
    resource_type = "network-interface"

    tags = {
      "github:runner-type" = var.runner_ci_services
    }
  }
}

# Auto Scaling Group
resource "aws_autoscaling_group" "ci_services" {
  name = var.runner_ci_services
  desired_capacity     = 2
  max_size             = 10
  min_size             = 0
  launch_template {
    id      = aws_launch_template.ci-services-template.id
    version = "$Latest"
  }

  #vpc_zone_identifier = ["subnet-12345678", "subnet-23456789"]  # Replace with your subnets
  #target_group_arns   = [aws_lb_target_group.tg.arn]            # Replace with your target group ARN if any

  health_check_type         = "EC2"
  health_check_grace_period = 300

  availability_zones        = ["us-east-1a", "us-east-1b", "us-east-1c", "us-east-1d", "us-east-1e", "us-east-1f"]
  # Suspend AZRebalance process
  suspended_processes = ["AZRebalance"]
}
